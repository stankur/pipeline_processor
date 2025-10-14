from __future__ import annotations
import math
import time
from typing import Literal
from datetime import datetime
from psycopg import Connection
from db import (
    get_user_subject,
    get_recommendation,
    bump_recommendation_exposures,
    get_repo_subject,
    get_user_languages,
    get_newest_trending_repo_age_hours,
    get_cached_recommendations_by_type,
)
from feed.sources import iter_highlight_repo_candidates, iter_trending_repo_candidates
from feed.trending import fetch_and_store_trending_repos
from feed.judge import judge_repo_for_user
from models import ForYouRepoItem, RepoSubject, Recommendation


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


def _apply_fatigue_and_create_item(
    repo: RepoSubject,
    author_username: str,
    github_ts: float,
    rec: Recommendation | None,
    now: float
) -> tuple[float, ForYouRepoItem]:
    """Calculate fatigue-adjusted score and create feed item.
    
    Returns: (adjusted_score, feed_item)
    """
    times_shown = rec.times_shown if rec else 0
    last_shown_at = rec.last_shown_at if rec else None
    
    hours_since = (now - float(last_shown_at)) / 3600.0 if last_shown_at else 1e9
    penalty_h = _fatigue_penalty_hours(times_shown, hours_since)
    adjusted_ts = github_ts - (penalty_h * 3600.0)
    
    feed_item = ForYouRepoItem(
        **repo.model_dump(),
        username=author_username,
        is_ghost=False,
    )
    
    return (adjusted_ts, feed_item)


def _finalize_ranked_feed(
    ranked: list[tuple[float, ForYouRepoItem]],
    limit: int,
    viewer_username: str,
    conn: Connection,
    now: float
) -> list[ForYouRepoItem]:
    """Sort, slice, bump exposures, return final feed."""
    # Sort by adjusted timestamp descending (most recent first)
    ranked.sort(key=lambda t: t[0], reverse=True)
    
    # Take top N
    top = ranked[:limit]
    items = [item for _, item in top]
    
    # Determine item_type from first item or default
    if items:
        # Infer item_type from feed source
        # Community repos have real usernames, trending have owners from GitHub
        item_type = "trending_repo" if not items[0].username or "/" not in items[0].id else "repo"
        shown_keys = [(item_type, item.id) for item in items]
    else:
        shown_keys = []
    
    # Bump exposure counters for shown items
    if shown_keys:
        bump_recommendation_exposures(conn, viewer_username, shown_keys, now)
    conn.commit()
    
    return items


def build_feed_for_user(
    conn: Connection,
    viewer_username: str,
    source: Literal["community", "trending"] = "community",
    limit: int = 30
) -> list[ForYouRepoItem]:
    """Build feed for user by judging candidates and ranking with LLM.
    
    Args:
        conn: Database connection
        viewer_username: Username to build feed for
        source: "community" for highlight repos, "trending" for GitHub trending
        limit: Max items to return
    
    Returns:
        List of ForYouRepoItem sorted by relevance
    """
    user = get_user_subject(conn, viewer_username)
    if not user:
        print(f"[rank] build_feed_for_user: user not found username={viewer_username} source={source}")
        return []
    
    # Get candidates based on source
    if source == "trending":
        # Check if we need to refresh trending repos
        age_hours = get_newest_trending_repo_age_hours(conn)
        if age_hours is None or age_hours > 6.0:
            print(f"[rank] Trending repos stale (age={age_hours}h), fetching fresh data...")
            languages = get_user_languages(conn, viewer_username)
            if languages:
                try:
                    fetch_and_store_trending_repos(conn, languages)
                except Exception as e:
                    print(f"[rank] Failed to fetch trending repos: {e}")
        
        candidates = list(iter_trending_repo_candidates(conn, viewer_username))
    else:  # community
        candidates = list(iter_highlight_repo_candidates(conn, viewer_username))
    
    now = time.time()
    print(f"[rank] build_feed_for_user: username={viewer_username} source={source} candidates={len(candidates)}")
    
    ranked: list[tuple[float, ForYouRepoItem]] = []
    included_count = 0
    excluded_count = 0
    
    for idx, (item_type, item_id, repo, author_username, github_ts) in enumerate(candidates, 1):
        print(f"[rank] Candidate {idx}/{len(candidates)}: {item_id} (author: @{author_username}, ts: {github_ts})")
        
        include = judge_repo_for_user(conn, user, repo, author_username, item_type)
        
        # Commit immediately so GET can see this judgment (incremental availability)
        conn.commit()
        
        if not include:
            print(f"[rank]   -> Judgment: EXCLUDE (skipped)")
            excluded_count += 1
            continue
        
        print(f"[rank]   -> Judgment: INCLUDE")
        included_count += 1
        
        rec = get_recommendation(conn, viewer_username, item_type, item_id)
        adjusted_ts, feed_item = _apply_fatigue_and_create_item(
            repo, author_username, github_ts, rec, now
        )
        
        times_shown = rec.times_shown if rec else 0
        hours_since = (now - float(rec.last_shown_at)) / 3600.0 if rec and rec.last_shown_at else 1e9
        penalty_h = _fatigue_penalty_hours(times_shown, hours_since)
        
        print(f"[rank]   -> Exposure: shown={times_shown}x, last={hours_since:.1f}h ago, penalty={penalty_h:.1f}h")
        print(f"[rank]   -> Score: {adjusted_ts:.2f} (raw: {github_ts:.2f})")
        
        ranked.append((adjusted_ts, feed_item))
    
    items = _finalize_ranked_feed(ranked, limit, viewer_username, conn, now)
    
    print(f"[rank] ===== FEED SUMMARY =====")
    print(f"[rank] Source: {source}")
    print(f"[rank] Candidates evaluated: {len(candidates)}")
    print(f"[rank] Included: {included_count}, Excluded: {excluded_count}")
    print(f"[rank] Final feed size: {len(items)}")
    print(f"[rank] ==========================")
    return items


def get_feed_from_cache(
    conn: Connection,
    viewer_username: str,
    source: Literal["community", "trending"] = "community",
    limit: int = 30
) -> list[ForYouRepoItem]:
    """Build feed from cached recommendations only (fast, no LLM calls).
    
    Args:
        conn: Database connection
        viewer_username: Username to build feed for
        source: "community" for highlight repos, "trending" for GitHub trending
        limit: Max items to return
    
    Queries recommendations table for items already judged as include=true,
    applies fatigue scoring, and returns top N items.
    
    Does NOT fetch candidates or run LLM judgments - purely reads from cache.
    """
    print(f"[rank] get_feed_from_cache: username={viewer_username} source={source}")
    
    # Determine item_type based on source
    item_type = "trending_repo" if source == "trending" else "repo"
    
    # Fetch cached recommendations
    recommendations = get_cached_recommendations_by_type(conn, viewer_username, item_type)
    print(f"[rank] Found {len(recommendations)} cached recommendations")
    
    if not recommendations:
        print(f"[rank] No cached recommendations found, returning empty feed")
        return []
    
    now = time.time()
    ranked: list[tuple[float, ForYouRepoItem]] = []
    
    for rec in recommendations:
        # Fetch repo subject (type depends on source)
        subject_type = "trending_repo" if source == "trending" else "repo"
        repo_row = conn.execute(
            "SELECT data_json FROM subjects WHERE subject_type=%s AND subject_id=%s",
            (subject_type, rec.item_id)
        ).fetchone()
        
        if not repo_row or not repo_row['data_json']:
            print(f"[rank] Skipping {rec.item_id} - repo subject not found")
            continue
        
        try:
            repo = RepoSubject.model_validate_json(repo_row['data_json'])
        except Exception as e:
            print(f"[rank] Skipping {rec.item_id} - failed to parse: {e}")
            continue
        
        # Extract GitHub timestamp
        if source == "trending":
            # Trending repos use extracted_at
            ts_str = repo.extracted_at
        else:
            # Community repos use pushed_at or updated_at
            ts_str = repo.pushed_at or repo.updated_at
        
        if not ts_str:
            print(f"[rank] Skipping {rec.item_id} - no timestamp")
            continue
        
        try:
            github_ts = datetime.fromisoformat(ts_str).timestamp()
        except Exception as e:
            print(f"[rank] Skipping {rec.item_id} - invalid timestamp: {e}")
            continue
        
        # Extract author/owner
        if source == "trending":
            # Extract owner from repo.id
            author_username = rec.item_id.split("/")[0] if "/" in rec.item_id else ""
        else:
            # Use stored username (could enhance by looking up in user_repo_links)
            author_username = rec.item_id.split("/")[0] if "/" in rec.item_id else ""
        
        adjusted_ts, feed_item = _apply_fatigue_and_create_item(
            repo, author_username, github_ts, rec, now
        )
        
        ranked.append((adjusted_ts, feed_item))
    
    items = _finalize_ranked_feed(ranked, limit, viewer_username, conn, now)
    
    print(f"[rank] Returned {len(items)} items from cache")
    return items
