"""
Dagster assets that wrap the existing task functions.
Each asset corresponds to one of the pipeline steps, preserving exact logic.
Uses ordering-only dependencies - assets read from DB, no data passing.
"""
import json
from typing import List, Optional

from dagster import asset, Config

from db import get_conn, get_work_item
from tasks import (
    fetch_profile,
    fetch_repos,
    select_highlighted_repos,
    infer_user_theme,
    enhance_repo_media,
    generate_repo_blurb,
    extract_repo_emphasis,
    extract_repo_keywords,
    extract_repo_kind,
)

class UserConfig(Config):
    username: str

@asset
def fetch_profile_asset(config: UserConfig) -> None:
    """Fetch GitHub profile for the user."""
    fetch_profile(config.username)


@asset
def fetch_repos_asset(config: UserConfig) -> None:
    """Fetch and filter repositories for the user."""
    fetch_repos(config.username)


@asset(deps=[fetch_repos_asset])
def select_highlighted_repos_asset(config: UserConfig) -> None:
    """Select highlighted repos - reads from DB, no data passing."""
    select_highlighted_repos(config.username)


@asset(deps=[select_highlighted_repos_asset])
def infer_user_theme_asset(config: UserConfig) -> None:
    """Infer user theme based on highlighted repos - reads from DB."""
    infer_user_theme(config.username)


@asset(deps=[select_highlighted_repos_asset])
def enhance_repo_media_asset(config: UserConfig) -> None:
    """Enhance media for each highlighted repo - reads highlighted repos from DB."""
    username = config.username
    
    # Read highlighted repos from DB
    conn = get_conn()
    work_item = get_work_item(conn, "select_highlighted_repos", "user", username)
    highlighted_repo_names = []
    if work_item and work_item["status"] == "succeeded" and work_item["output_json"]:
        try:
            data = json.loads(work_item["output_json"])
            highlighted_repo_names = data.get("repos", [])
        except Exception:
            pass
    
    # Get repo IDs from highlighted repos (need to convert names to full IDs)
    from db import get_user_repos
    user_repos = get_user_repos(conn, username)
    repo_by_name = {}
    for repo_row in user_repos:
        try:
            repo_data = json.loads(repo_row["data_json"]) if repo_row["data_json"] else {}
            name = repo_data.get("name")
            if name:
                repo_by_name[name] = repo_row["subject_id"]
        except Exception:
            continue
    
    # Enhance each highlighted repo
    for repo_name in highlighted_repo_names:
        repo_id = repo_by_name.get(repo_name)
        if repo_id:
            enhance_repo_media(repo_id)


@asset(deps=[select_highlighted_repos_asset])
def generate_repo_blurb_asset(config: UserConfig) -> None:
    """Generate blurbs for each highlighted repo - reads highlighted repos from DB."""
    username = config.username
    
    # Read highlighted repos from DB
    conn = get_conn()
    work_item = get_work_item(conn, "select_highlighted_repos", "user", username)
    highlighted_repo_names = []
    if work_item and work_item["status"] == "succeeded" and work_item["output_json"]:
        try:
            data = json.loads(work_item["output_json"])
            highlighted_repo_names = data.get("repos", [])
        except Exception:
            pass
    
    # Get repo IDs from highlighted repos (need to convert names to full IDs)
    from db import get_user_repos
    user_repos = get_user_repos(conn, username)
    repo_by_name = {}
    for repo_row in user_repos:
        try:
            repo_data = json.loads(repo_row["data_json"]) if repo_row["data_json"] else {}
            name = repo_data.get("name")
            if name:
                repo_by_name[name] = repo_row["subject_id"]
        except Exception:
            continue
    
    # Generate blurb for each highlighted repo
    for repo_name in highlighted_repo_names:
        repo_id = repo_by_name.get(repo_name)
        if repo_id:
            generate_repo_blurb(repo_id)


@asset(deps=[generate_repo_blurb_asset])
def extract_repo_emphasis_asset(config: UserConfig) -> None:
    """Extract emphasis only for repos that successfully generated blurbs."""
    username = config.username
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT w.subject_id
        FROM work_items w
        JOIN user_repo_links l ON l.repo_id = w.subject_id AND l.username = %s
        WHERE w.kind = 'generate_repo_blurb'
          AND w.subject_type = 'repo'
          AND w.status = 'succeeded'
        """,
        (username,),
    ).fetchall()
    for row in rows:
        repo_id = row["subject_id"]
        extract_repo_emphasis(repo_id)


@asset(deps=[generate_repo_blurb_asset])
def extract_repo_keywords_asset(config: UserConfig) -> None:
    """Extract keywords only for repos that successfully generated blurbs."""
    username = config.username
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT w.subject_id
        FROM work_items w
        JOIN user_repo_links l ON l.repo_id = w.subject_id AND l.username = %s
        WHERE w.kind = 'generate_repo_blurb'
          AND w.subject_type = 'repo'
          AND w.status = 'succeeded'
        """,
        (username,),
    ).fetchall()
    for row in rows:
        repo_id = row["subject_id"]
        extract_repo_keywords(repo_id)


@asset(deps=[generate_repo_blurb_asset])
def extract_repo_kind_asset(config: UserConfig) -> None:
    """Extract a compact kind phrase only for repos that successfully generated blurbs."""
    username = config.username
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT w.subject_id
        FROM work_items w
        JOIN user_repo_links l ON l.repo_id = w.subject_id AND l.username = %s
        WHERE w.kind = 'generate_repo_blurb'
          AND w.subject_type = 'repo'
          AND w.status = 'succeeded'
        """,
        (username,),
    ).fetchall()
    for row in rows:
        repo_id = row["subject_id"]
        extract_repo_kind(repo_id)
