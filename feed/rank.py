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
    get_trending_repos_age_for_languages,
    get_repos_similarity_to_user,
    get_all_trending_repos,
    get_users_similarity_to_user,
)
from feed.sources import iter_highlight_repo_candidates, iter_trending_repo_candidates, iter_user_candidates
from feed.trending import fetch_and_store_trending_repos
from models import ForYouRepoItem, RepoSubject, Recommendation, UserSubject, ForYouUserItem
from tasks import embed_trending_repos_batch


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
    recency = (
        beta * math.exp(-hours_since_last / tau)
        if hours_since_last is not None
        else 0.0
    )
    return base + recency


def build_feed_for_user(
    conn: Connection,
    viewer_username: str,
    source: Literal["community", "trending"] = "community",
    limit: int = 30,
) -> list[ForYouRepoItem]:
    """Build feed for user using embedding similarity.

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
        print(
            f"[rank] build_feed_for_user: user not found username={viewer_username} source={source}"
        )
        return []

    return _build_feed_embeddings(conn, viewer_username, source, limit)


def _build_feed_embeddings(
    conn: Connection,
    viewer_username: str,
    source: Literal["community", "trending"] = "community",
    limit: int = 30,
) -> list[ForYouRepoItem]:
    """Build feed using embedding similarity ranking.

    Fast path: computes cosine similarity between user and repo embeddings,
    applies fatigue penalty, returns top N. Considers all candidates (no sampling).

    Args:
        source: "community" for highlight repos, "trending" for GitHub trending
    """
    now = time.time()

    # Get candidates based on source
    if source == "community":
        candidates = list(iter_highlight_repo_candidates(conn, viewer_username))
        subject_type = "repo"
    else:  # trending
        # Check if we need to refresh trending repos for user's languages
        languages = get_user_languages(conn, viewer_username)
        if languages:
            age_hours = get_trending_repos_age_for_languages(conn, languages)
            if age_hours is None or age_hours > 6.0:
                print(
                    f"[rank] Trending repos stale for languages {languages} (age={age_hours}h), fetching fresh data..."
                )
                try:
                    fetch_and_store_trending_repos(conn, languages)

                    # Immediately embed new trending repos
                    trending_repos = get_all_trending_repos(conn)
                    repos_to_embed = [
                        r["subject_id"]
                        for r in trending_repos
                        if not r.get("embedding")
                    ]
                    if repos_to_embed:
                        print(
                            f"[rank] Embedding {len(repos_to_embed)} new trending repos..."
                        )
                        embed_trending_repos_batch(repos_to_embed)
                except Exception as e:
                    print(f"[rank] Failed to fetch/embed trending repos: {e}")

        candidates = list(iter_trending_repo_candidates(conn, viewer_username))
        subject_type = "trending_repo"

    print(
        f"[rank] build_feed_embeddings: username={viewer_username} source={source} candidates={len(candidates)}"
    )

    if not candidates:
        print(f"[rank] No candidates found")
        return []

    # Extract repo IDs for similarity computation
    candidate_repo_ids = [
        item_id for item_type, item_id, repo, author, ts in candidates
    ]

    # Compute similarities
    similarity_scores = get_repos_similarity_to_user(
        conn, viewer_username, candidate_repo_ids, subject_type
    )

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
        penalty_hours = _fatigue_penalty_hours(times_shown/5, hours_since)
        similarity_penalty = penalty_hours / 500.0  # 240 hours = 1.0 similarity point

        # Apply fatigue to similarity
        adjusted_score = similarity - similarity_penalty

        # Create feed item
        feed_item = ForYouRepoItem(
            **repo.model_dump(),
            username=author_username,
            is_ghost=False,
        )

        ranked.append((adjusted_score, feed_item, item_type, item_id))

        print(
            f"[rank] {item_id}: similarity={similarity:.3f}, shown={times_shown}x, penalty={penalty_hours:.1f}h, adjusted={adjusted_score:.3f}"
        )

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
        shown_keys = [
            (top[i][2], top[i][3]) for i in range(len(top))
        ]  # (item_type, item_id)
        bump_recommendation_exposures(conn, viewer_username, shown_keys, now)

    conn.commit()

    print(f"[rank] ===== FEED SUMMARY (EMBEDDINGS) =====")
    print(f"[rank] Source: {source}")
    print(f"[rank] Candidates evaluated: {len(candidates)}")
    print(f"[rank] With embeddings: {len(ranked)}")
    print(
        f"[rank] After dedup: {len(deduped)} (removed {len(ranked) - len(deduped)} duplicates)"
    )
    print(f"[rank] Final feed size: {len(items)}")
    print(f"[rank] ========================================")

    return items


def build_user_feed(
    conn: Connection,
    viewer_username: str,
    limit: int = 30,
) -> list[ForYouUserItem]:
    """Build user recommendation feed using embedding similarity.

    Args:
        conn: Database connection
        viewer_username: Username to build feed for
        limit: Max users to return

    Returns:
        List of ForYouUserItem sorted by similarity (descending)
    """
    user = get_user_subject(conn, viewer_username)
    if not user:
        print(f"[rank] build_user_feed: viewer not found username={viewer_username}")
        return []

    # Get all user candidates
    candidates = list(iter_user_candidates(conn, viewer_username))
    
    print(f"[rank] build_user_feed: username={viewer_username} candidates={len(candidates)}")

    if not candidates:
        print(f"[rank] No user candidates found")
        return []

    # Extract usernames for similarity computation
    candidate_usernames = [username for item_type, username, user_model, ts in candidates]

    # Compute similarities
    similarity_scores = get_users_similarity_to_user(conn, viewer_username, candidate_usernames)

    if not similarity_scores:
        print(f"[rank] No similarity scores computed (viewer or candidate embeddings missing)")
        return []

    # Build ranked list (no fatigue penalty)
    ranked: list[tuple[float, ForYouUserItem]] = []

    for item_type, username, user_model, timestamp in candidates:
        similarity = similarity_scores.get(username)
        if similarity is None:
            continue

        feed_item = ForYouUserItem(**user_model.model_dump())
        ranked.append((similarity, feed_item))

        print(f"[rank] {username}: similarity={similarity:.3f}")

    # Sort by similarity (descending)
    ranked.sort(key=lambda t: t[0], reverse=True)

    # Take top N
    top = ranked[:limit]
    items = [item for score, item in top]

    print(f"[rank] ===== USER FEED SUMMARY =====")
    print(f"[rank] Candidates evaluated: {len(candidates)}")
    print(f"[rank] With embeddings: {len(ranked)}")
    print(f"[rank] Final feed size: {len(items)}")
    print(f"[rank] ============================")

    return items
