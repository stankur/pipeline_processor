from __future__ import annotations
import math
import time
from datetime import datetime
from psycopg import Connection
from db import get_user_subject, get_recommendation, bump_recommendation_exposures, get_cached_recommendations, get_repo_subject
from feed.sources import iter_highlight_repo_candidates
from feed.judge import get_or_create_judgment_for_repo
from models import ForYouRepoItem


def _fatigue_penalty_hours(
    times_shown: int,
    hours_since_last: float,
    alpha: float = 18.0,
    beta: float = 24.0,
    tau: float = 24.0,
) -> float:
    """Compute fatigue penalty in hours to subtract from item recency.
    
    Args:
        times_shown: number of times item was shown to user
        hours_since_last: hours since last shown (or large number if never shown)
        alpha: penalty multiplier per exposure (default 18h per show)
        beta: immediate recency penalty magnitude (default 24h)
        tau: decay time constant for recency penalty (default 24h)
    
    Returns:
        float: penalty in hours to subtract from item's github timestamp
    
    This produces smooth fatigue:
    - Each prior exposure adds ~18h penalty
    - Recently shown items get additional exponential penalty that decays over ~24h
    - No hard caps; items can reappear but are pushed down proportionally
    """
    base = alpha * max(0, int(times_shown or 0))
    recency = beta * math.exp(-hours_since_last / tau) if hours_since_last is not None else 0.0
    return base + recency


def get_feed_from_cache(conn: Connection, viewer_username: str, limit: int = 30) -> list[ForYouRepoItem]:
    """Build feed from cached recommendations only (fast, no LLM calls).
    
    Queries recommendations table for items already judged as include=true,
    applies fatigue scoring, and returns top N items.
    
    Does NOT fetch candidates or run LLM judgments - purely reads from cache.
    """
    print(f"[rank] get_feed_from_cache: username={viewer_username}")
    
    # Fetch cached recommendations where include=true
    recommendations = get_cached_recommendations(conn, viewer_username)
    print(f"[rank] Found {len(recommendations)} cached recommendations")
    
    if not recommendations:
        print(f"[rank] No cached recommendations found, returning empty feed")
        return []
    
    now = time.time()
    ranked: list[tuple[float, ForYouRepoItem]] = []
    
    for rec in recommendations:
        # Only handle repo type for now
        if rec.item_type != "repo":
            continue
        
        # Fetch repo subject
        repo = get_repo_subject(conn, rec.item_id)
        if not repo:
            print(f"[rank] Skipping {rec.item_id} - repo subject not found")
            continue
        
        # Extract GitHub timestamp from repo
        ts_str = repo.pushed_at or repo.updated_at
        if not ts_str:
            print(f"[rank] Skipping {rec.item_id} - no timestamp")
            continue
        
        try:
            github_ts = datetime.fromisoformat(ts_str).timestamp()
        except Exception as e:
            print(f"[rank] Skipping {rec.item_id} - invalid timestamp: {e}")
            continue
        
        # Calculate fatigue penalty
        hours_since = (now - float(rec.last_shown_at)) / 3600.0 if rec.last_shown_at else 1e9
        penalty_h = _fatigue_penalty_hours(rec.times_shown, hours_since)
        adjusted_ts = github_ts - (penalty_h * 3600.0)
        
        # Extract author username from repo.id (owner/repo)
        author_username = rec.item_id.split("/")[0] if "/" in rec.item_id else ""
        
        feed_item = ForYouRepoItem(
            **repo.model_dump(),
            username=author_username,
            is_ghost=False,
        )
        
        ranked.append((adjusted_ts, feed_item))
    
    # Sort by adjusted timestamp descending
    ranked.sort(key=lambda t: t[0], reverse=True)
    
    # Take top N
    top = ranked[:limit]
    items = [item for _, item in top]
    
    # Bump exposure counters
    shown_keys = [("repo", item.id) for item in items]
    bump_recommendation_exposures(conn, viewer_username, shown_keys, now)
    conn.commit()
    
    print(f"[rank] Returned {len(items)} items from cache")
    return items


def build_feed_for_user(conn: Connection, viewer_username: str, limit: int = 30) -> list[ForYouRepoItem]:
    user = get_user_subject(conn, viewer_username)
    if not user:
        print(f"[rank] build_feed_for_user: user not found username={viewer_username}")
        return []
    
    now = time.time()
    candidates = list(iter_highlight_repo_candidates(conn, viewer_username))
    print(f"[rank] build_feed_for_user: username={viewer_username} candidates={len(candidates)}")
    
    ranked: list[tuple[float, ForYouRepoItem]] = []
    included_count = 0
    excluded_count = 0
    
    for idx, (item_type, item_id, repo, author_username, github_ts) in enumerate(candidates, 1):
        print(f"[rank] Candidate {idx}/{len(candidates)}: {item_id} (author: @{author_username}, ts: {github_ts})")
        
        include = get_or_create_judgment_for_repo(conn, user, repo, author_username)
        
        # Commit immediately so GET can see this judgment (incremental availability)
        conn.commit()
        
        if not include:
            print(f"[rank]   -> Judgment: EXCLUDE (skipped)")
            excluded_count += 1
            continue
        
        print(f"[rank]   -> Judgment: INCLUDE")
        included_count += 1
        
        rec = get_recommendation(conn, viewer_username, item_type, item_id)
        times_shown = rec.times_shown if rec else 0
        last_shown_at = rec.last_shown_at if rec else None
        
        hours_since = (now - float(last_shown_at)) / 3600.0 if last_shown_at else 1e9
        
        penalty_h = _fatigue_penalty_hours(times_shown, hours_since)
        adjusted_ts = github_ts - (penalty_h * 3600.0)
        
        print(f"[rank]   -> Exposure: shown={times_shown}x, last={hours_since:.1f}h ago, penalty={penalty_h:.1f}h")
        print(f"[rank]   -> Score: {adjusted_ts:.2f} (raw: {github_ts:.2f})")
        
        feed_item = ForYouRepoItem(
            **repo.model_dump(),
            username=author_username,
            is_ghost=False,  # Could fetch from user subject if needed
        )
        
        ranked.append((adjusted_ts, feed_item))
    
    # Sort by adjusted timestamp descending (most recent first)
    ranked.sort(key=lambda t: t[0], reverse=True)
    
    # Take top N
    top = ranked[:limit]
    items = [item for _, item in top]
    
    # Bump exposure counters for shown items
    shown_keys = [("repo", item.id) for item in items]
    bump_recommendation_exposures(conn, viewer_username, shown_keys, now)
    conn.commit()
    
    print(f"[rank] ===== FEED SUMMARY =====")
    print(f"[rank] Candidates evaluated: {len(candidates)}")
    print(f"[rank] Included: {included_count}, Excluded: {excluded_count}")
    print(f"[rank] Final feed size: {len(items)}")
    print(f"[rank] ==========================")
    return items

