"""Serialization for LLM input (minimal, stable, fingerprint-safe)."""
from __future__ import annotations
from typing import Any
from psycopg import Connection
from models import UserSubject, RepoSubject
from db import get_repo_subject


def serialize_user_profile_minimal(conn: Connection, user: UserSubject) -> dict[str, Any]:
    """Serialize user profile with full highlight details for LLM.
    
    For each highlighted repo, fetches the full repo subject to include:
    - name, description, generated_description, language
    """
    highlights = []
    for repo_name in (user.highlighted_repos or []):
        # Try to find this repo in user's repos (could be owned or contributed)
        # We need the full repo_id (owner/repo) to fetch it
        # For now, assume it's owned by this user (we'll need to improve this)
        repo_id = f"{user.login}/{repo_name}"
        repo = get_repo_subject(conn, repo_id)
        
        if repo:
            highlights.append({
                "name": repo.name,
                "description": (repo.description or "").strip(),
                "generated_description": (repo.generated_description or "").strip(),
                "language": repo.language or "",
            })
    
    return {
        "username": user.login,
        "bio": (user.bio or "").strip(),
        "highlights": highlights,
    }


def serialize_repo_for_llm(repo: RepoSubject, author_username: str) -> dict[str, Any]:
    return {
        "name": repo.name,
        "author": author_username,
        "description": (repo.description or "").strip(),
        "generated_description": (repo.generated_description or "").strip(),
        "language": repo.language or "",
    }

