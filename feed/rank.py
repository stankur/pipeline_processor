from __future__ import annotations
import math
import random
import time
from typing import Literal
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg import Connection
from db import (
    get_conn,
    get_user_subject,
    get_recommendation,
    bump_recommendation_exposures,
    get_repo_subject,
    get_user_languages,
    get_newest_trending_repo_age_hours,
    get_cached_recommendations_by_type,
    get_repos_similarity_to_user,
)
from feed.sources import iter_highlight_repo_candidates, iter_trending_repo_candidates
from feed.trending import fetch_and_store_trending_repos
from feed.judge import judge_repo_for_user
from models import ForYouRepoItem, RepoSubject, Recommendation, UserSubject


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


def _judge_candidate_parallel(
    viewer_user,
    viewer_username: str,
    candidate_tuple: tuple,
    now: float
) -> tuple | None:
    """Judge a single candidate in parallel (thread-safe).
    
    Returns:
        tuple: (adjusted_ts, feed_item, item_type, item_id) if included
        None: if excluded
    """
    item_type, item_id, repo, author_username, github_ts = candidate_tuple
    
    # Get fresh connection for this thread (psycopg creates new conn each time)
    conn = get_conn()
    
    try:
        # Judge the repo (may call LLM or use cache)
        include = judge_repo_for_user(conn, viewer_user, repo, author_username, item_type)
        
        if not include:
            print(f"[rank]   -> {item_id}: EXCLUDE")
            return None
        
        print(f"[rank]   -> {item_id}: INCLUDE")
        
        # Get recommendation and compute fatigue score
        rec = get_recommendation(conn, viewer_username, item_type, item_id)
        adjusted_ts, feed_item = _apply_fatigue_and_create_item(
            repo, author_username, github_ts, rec, now
        )
        
        times_shown = rec.times_shown if rec else 0
        hours_since = (now - float(rec.last_shown_at)) / 3600.0 if rec and rec.last_shown_at else 1e9
        penalty_h = _fatigue_penalty_hours(times_shown, hours_since)
        
        print(f"[rank]   -> {item_id}: shown={times_shown}x, penalty={penalty_h:.1f}h, score={adjusted_ts:.2f}")
        
        return (adjusted_ts, feed_item, item_type, item_id)
    
    finally:
        # Commit this thread's transaction before closing
        conn.commit()
        conn.close()


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
    limit: int = 30,
    sample_n: int | None = None,
    batch_size: int = 15
) -> list[ForYouRepoItem]:
    """Build feed for user using embedding similarity (community) or LLM judgment (trending).
    
    Args:
        conn: Database connection
        viewer_username: Username to build feed for
        source: "community" for highlight repos, "trending" for GitHub trending
        limit: Max items to return
        sample_n: Number of candidates to sample (trending only, ignored for community)
        batch_size: Number of candidates to judge in parallel per batch (trending only)
    
    Returns:
        List of ForYouRepoItem sorted by relevance
    """
    user = get_user_subject(conn, viewer_username)
    if not user:
        print(f"[rank] build_feed_for_user: user not found username={viewer_username} source={source}")
        return []
    
    if source == "community":
        # NEW: Fast embedding-based ranking (no LLM calls, no sampling)
        return _build_feed_embeddings(conn, viewer_username, limit)
    else:
        # OLD: LLM-based judgment for trending (keeps existing logic)
        return _build_feed_llm(conn, viewer_username, user, source, limit, sample_n, batch_size)


def _build_feed_embeddings(
    conn: Connection,
    viewer_username: str,
    limit: int = 30
) -> list[ForYouRepoItem]:
    """Build community feed using embedding similarity ranking.
    
    Fast path: computes cosine similarity between user and repo embeddings,
    applies fatigue penalty, returns top N. Considers all candidates (no sampling).
    """
    now = time.time()
    
    # Get all candidates
    candidates = list(iter_highlight_repo_candidates(conn, viewer_username))
    print(f"[rank] build_feed_embeddings: username={viewer_username} candidates={len(candidates)}")
    
    if not candidates:
        print(f"[rank] No candidates found")
        return []
    
    # Extract repo IDs for similarity computation
    candidate_repo_ids = [item_id for item_type, item_id, repo, author, ts in candidates]
    
    # Compute similarities
    similarity_scores = get_repos_similarity_to_user(conn, viewer_username, candidate_repo_ids)
    
    if not similarity_scores:
        print(f"[rank] No similarity scores computed (user or repo embeddings missing)")
        return []
    
    # Build ranked list with fatigue adjustment
    ranked: list[tuple[float, ForYouRepoItem, str, str]] = []
    
    for item_type, item_id, repo, author_username, github_ts in candidates:
        # Skip repos without embeddings
        similarity = similarity_scores.get(item_id)
        if similarity is None:
            continue
        
        # Get exposure data
        rec = get_recommendation(conn, viewer_username, item_type, item_id)
        times_shown = rec.times_shown if rec else 0
        last_shown_at = rec.last_shown_at if rec else None
        
        # Compute fatigue penalty (convert hours to similarity penalty)
        hours_since = (now - float(last_shown_at)) / 3600.0 if last_shown_at else 1e9
        penalty_hours = _fatigue_penalty_hours(times_shown, hours_since)
        similarity_penalty = penalty_hours / 240.0  # 240 hours = 1.0 similarity point
        
        # Apply fatigue to similarity
        adjusted_score = similarity - similarity_penalty
        
        # Create feed item
        feed_item = ForYouRepoItem(
            **repo.model_dump(),
            username=author_username,
            is_ghost=False,
        )
        
        ranked.append((adjusted_score, feed_item, item_type, item_id))
        
        print(f"[rank] {item_id}: similarity={similarity:.3f}, shown={times_shown}x, penalty={penalty_hours:.1f}h, adjusted={adjusted_score:.3f}")
    
    # Sort by adjusted score (descending)
    ranked.sort(key=lambda t: t[0], reverse=True)
    
    # Dedup by repo_id (keep highest scoring instance)
    seen_repo_ids = set()
    deduped = []
    for score, item, item_type, item_id in ranked:
        if item_id not in seen_repo_ids:
            seen_repo_ids.add(item_id)
            deduped.append((score, item, item_type, item_id))
    
    # Take top N
    top = deduped[:limit]
    items = [item for score, item, item_type, item_id in top]
    
    # Bump exposure counters for shown items
    if items:
        shown_keys = [(top[i][2], top[i][3]) for i in range(len(top))]  # (item_type, item_id)
        bump_recommendation_exposures(conn, viewer_username, shown_keys, now)
    
    conn.commit()
    
    print(f"[rank] ===== FEED SUMMARY (EMBEDDINGS) =====")
    print(f"[rank] Candidates evaluated: {len(candidates)}")
    print(f"[rank] With embeddings: {len(ranked)}")
    print(f"[rank] After dedup: {len(deduped)} (removed {len(ranked) - len(deduped)} duplicates)")
    print(f"[rank] Final feed size: {len(items)}")
    print(f"[rank] ========================================")
    
    return items


def _build_feed_llm(
    conn: Connection,
    viewer_username: str,
    user: UserSubject,
    source: str,
    limit: int = 30,
    sample_n: int | None = None,
    batch_size: int = 15
) -> list[ForYouRepoItem]:
    """Build feed using LLM judgment (for trending repos).
    
    Original logic: parallel LLM calls with caching.
    """
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
    else:
        candidates = []
    
    now = time.time()
    original_count = len(candidates)
    print(f"[rank] build_feed_llm: username={viewer_username} source={source} candidates={original_count}")
    
    if sample_n is not None and len(candidates) > sample_n:
        candidates = random.sample(candidates, sample_n)
        print(f"[rank] Sampled {sample_n} candidates from {original_count} total")
    
    ranked: list[tuple[float, ForYouRepoItem]] = []
    included_count = 0
    excluded_count = 0
    
    # Process candidates in batches with parallelism
    total_batches = (len(candidates) + batch_size - 1) // batch_size
    
    for batch_idx in range(0, len(candidates), batch_size):
        batch = candidates[batch_idx:batch_idx + batch_size]
        batch_num = (batch_idx // batch_size) + 1
        
        print(f"[rank] === Batch {batch_num}/{total_batches} ({len(batch)} candidates) ===")
        
        # Process batch in parallel
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Submit all candidates in this batch
            future_to_candidate = {
                executor.submit(_judge_candidate_parallel, user, viewer_username, cand, now): cand
                for cand in batch
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_candidate):
                candidate = future_to_candidate[future]
                item_id = candidate[1]  # Extract item_id for logging
                
                try:
                    result = future.result()
                    if result:
                        adjusted_ts, feed_item, item_type, item_id_result = result
                        ranked.append((adjusted_ts, feed_item))
                        included_count += 1
                    else:
                        excluded_count += 1
                except Exception as exc:
                    print(f"[rank] ERROR judging {item_id}: {exc}")
                    excluded_count += 1
        
        # Commit after each batch (incremental availability)
        conn.commit()
        print(f"[rank] Batch {batch_num} complete, committed {len(batch)} judgments")
    
    items = _finalize_ranked_feed(ranked, limit, viewer_username, conn, now)
    
    print(f"[rank] ===== FEED SUMMARY (LLM) =====")
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
    
    Used only for trending feed now. Community feed uses embeddings directly.
    
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
    
    # Community feed now uses embeddings directly
    if source == "community":
        return build_feed_for_user(conn, viewer_username, source="community", limit=limit)
    
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
