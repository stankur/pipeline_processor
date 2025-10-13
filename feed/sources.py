"""Candidate sources for For You feed."""
from __future__ import annotations
from typing import Iterator, Tuple
from datetime import datetime
from psycopg import Connection
from models import UserSubject, RepoSubject
from db import get_user_repos, select_best_repo_candidate, iter_users_with_highlights

# Item tuple: (item_type, item_id, repo_model, author_username, github_timestamp)
Item = Tuple[str, str, RepoSubject, str, float]


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
            
            # Exclude repos owned by the viewer
            viewer_owner_prefix = f"{viewer_username}/"
            if isinstance(repo.id, str) and repo.id.startswith(viewer_owner_prefix):
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

