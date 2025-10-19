"""Candidate sources for For You feed."""
from __future__ import annotations
from typing import Iterator, Tuple
from datetime import datetime
from psycopg import Connection
from models import UserSubject, RepoSubject, ItemType, ForYouUserItem
from db import (
    get_user_repos,
    select_best_repo_candidate,
    iter_users_with_highlights,
    get_user_subject,
    get_user_languages,
    get_trending_repos_by_languages,
    get_all_users_except_viewer,
)

# Item tuple: (item_type, item_id, repo_model, author_username, github_timestamp)
Item = Tuple[ItemType, str, RepoSubject, str, float]


def iter_highlight_repo_candidates(conn: Connection, viewer_username: str) -> Iterator[Item]:
    """Yield highlight repo candidates for the viewer, excluding repos owned by viewer.
    
    For each user with highlighted_repos, resolve to actual repo subjects and yield:
    - item_type: 'repo'
    - item_id: repo.id (owner/repo)
    - repo_model: RepoSubject
    - author_username: the user whose highlights include this repo
    - github_timestamp: pushed_at or updated_at as epoch seconds
    
    Selection logic (matches existing /for-you endpoint):
    - If multiple repos have same name for a user, pick by:
      1) highest user_commit_days (None -> -1)
      2) prefer external original (owner != user)
      3) newest subjects.updated_at
    """
    for author_username, user in iter_users_with_highlights(conn):
        repo_rows = get_user_repos(conn, author_username)
        
        # Index by bare name (like existing for-you logic)
        name_to_candidates: dict[str, list[tuple[RepoSubject, dict]]] = {}
        for rr in repo_rows:
            data_json = rr.get("data_json")
            if not data_json:
                continue
            try:
                repo = RepoSubject.model_validate_json(data_json)
            except Exception:
                continue
            name_to_candidates.setdefault(repo.name, []).append((repo, rr))
        
        for repo_name in user.highlighted_repos:
            candidates = name_to_candidates.get(repo_name)
            if not candidates:
                continue
            
            repo, rr = select_best_repo_candidate(candidates, author_username)
            
            # Exclude repos owned by the viewer (case-insensitive)
            if isinstance(repo.id, str) and '/' in repo.id:
                owner = repo.id.split('/')[0]
                if owner.lower() == viewer_username.lower():
                    continue
            
            # Extract GitHub timestamp
            ts_str = repo.pushed_at or repo.updated_at
            if not ts_str:
                continue
            try:
                github_ts = datetime.fromisoformat(ts_str).timestamp()
            except Exception:
                continue
            
            yield ("repo", repo.id, repo, author_username, github_ts)


def iter_trending_repo_candidates(conn: Connection, viewer_username: str) -> Iterator[Item]:
    """Yield trending repo candidates filtered by viewer's languages.
    
    Yields repos from subjects table with subject_type='trending_repo' that match
    any language used in the viewer's highlighted repos.
    
    Yields:
        - item_type: 'trending_repo'
        - item_id: repo.id (owner/repo)
        - repo_model: RepoSubject
        - author_username: owner extracted from repo.id
        - github_timestamp: extracted_at as epoch seconds
    """
    # Get viewer's user subject to access highlighted repos
    user = get_user_subject(conn, viewer_username)
    if not user or not user.highlighted_repos:
        print(f"[sources] iter_trending_repo_candidates: no highlighted repos for {viewer_username}")
        return
    
    # Extract languages from viewer's repos using db function
    languages = get_user_languages(conn, viewer_username)
    
    if not languages:
        print(f"[sources] iter_trending_repo_candidates: no languages found for {viewer_username}")
        return
    
    print(f"[sources] iter_trending_repo_candidates: viewer languages={languages}")
    
    # Query trending repos matching viewer's languages using db function
    rows = get_trending_repos_by_languages(conn, languages)
    
    for row in rows:
        repo_id = row["subject_id"]
        data_json = row.get("data_json")
        if not data_json:
            continue
        
        try:
            repo = RepoSubject.model_validate_json(data_json)
        except Exception:
            continue
        
        # Trending repos must have extracted_at
        if not repo.extracted_at:
            print(f"[sources] ERROR: Trending repo {repo.id} missing extracted_at, skipping")
            continue
        
        try:
            github_ts = datetime.fromisoformat(repo.extracted_at).timestamp()
        except Exception as e:
            print(f"[sources] ERROR: Invalid extracted_at for {repo.id}: {e}")
            continue
        
        # Extract owner from repo.id (e.g., "anthropics/prompt-eng-interactive-tutorial")
        owner = repo.id.split('/')[0] if '/' in repo.id else ""
        
        yield ("trending_repo", repo.id, repo, owner, github_ts)


def iter_user_candidates(conn: Connection, viewer_username: str) -> Iterator[Tuple[ItemType, str, UserSubject, float]]:
    """Yield all user candidates for viewer, excluding viewer themselves (case-insensitive).
    
    Yields:
        - item_type: 'user'
        - item_id: username (login)
        - user_model: UserSubject
        - timestamp: subjects.updated_at as epoch seconds
    """
    rows = get_all_users_except_viewer(conn, viewer_username)
    
    for row in rows:
        username = row["subject_id"]
        data_json = row.get("data_json")
        updated_at = row.get("updated_at")
        
        if not data_json or not updated_at:
            continue
        
        try:
            user = UserSubject.model_validate_json(data_json)
        except Exception:
            continue
        
        yield ("user", username, user, float(updated_at))

