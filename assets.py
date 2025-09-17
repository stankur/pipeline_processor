"""
Dagster assets that wrap the existing task functions.
Each asset corresponds to one of the pipeline steps, preserving exact logic.
"""
import json
from typing import List, Optional

from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition

from db import get_conn, get_work_item
from tasks import (
    fetch_profile,
    fetch_repos,
    select_highlighted_repos,
    infer_user_theme,
    enhance_repo_media,
    generate_repo_blurb,
    finalize_user_profile,
)

# Define partitions for users
users_partitions = StaticPartitionsDefinition(["stankur", "ProjectsByJackHe", "SamuelmdLow"])

@asset(partitions_def=users_partitions)
def fetch_profile_asset(context: AssetExecutionContext) -> None:
    """Fetch GitHub profile for the user."""
    username = context.partition_key
    fetch_profile(username)


@asset(partitions_def=users_partitions)
def fetch_repos_asset(context: AssetExecutionContext) -> None:
    """Fetch and filter repositories for the user."""
    username = context.partition_key
    fetch_repos(username)


@asset(partitions_def=users_partitions)
def select_highlighted_repos_asset(
    context: AssetExecutionContext, fetch_repos_asset
) -> List[str]:
    """Select highlighted repos and return the list for downstream assets."""
    username = context.partition_key
    select_highlighted_repos(username)
    
    # Read the result from work_items to return for downstream assets
    conn = get_conn()
    work_item = get_work_item(conn, "select_highlighted_repos", "user", username)
    if work_item and work_item["status"] == "succeeded" and work_item["output_json"]:
        try:
            data = json.loads(work_item["output_json"])
            repos = data.get("repos", [])
            return repos if isinstance(repos, list) else []
        except Exception:
            return []
    return []


@asset(partitions_def=users_partitions)
def infer_user_theme_asset(
    context: AssetExecutionContext, select_highlighted_repos_asset: List[str]
) -> str:
    """Infer user theme based on highlighted repos."""
    username = context.partition_key
    infer_user_theme(username)
    
    # Read the result to return theme
    conn = get_conn()
    work_item = get_work_item(conn, "infer_user_theme", "user", username)
    if work_item and work_item["status"] == "succeeded" and work_item["output_json"]:
        try:
            data = json.loads(work_item["output_json"])
            return data.get("theme", "")
        except Exception:
            return ""
    return ""


@asset(partitions_def=users_partitions)
def enhance_repo_media_asset(
    context: AssetExecutionContext, select_highlighted_repos_asset: List[str]
) -> None:
    """Enhance media for each highlighted repo."""
    username = context.partition_key
    
    # Get repo IDs from highlighted repos (need to convert names to full IDs)
    from db import get_user_repos
    conn = get_conn()
    
    # Get all user repos to map names to IDs
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
    for repo_name in select_highlighted_repos_asset:
        repo_id = repo_by_name.get(repo_name)
        if repo_id:
            enhance_repo_media(repo_id)


@asset(partitions_def=users_partitions)
def generate_repo_blurb_asset(
    context: AssetExecutionContext, select_highlighted_repos_asset: List[str]
) -> None:
    """Generate blurbs for each highlighted repo."""
    username = context.partition_key
    
    # Get repo IDs from highlighted repos (need to convert names to full IDs)
    from db import get_user_repos
    conn = get_conn()
    
    # Get all user repos to map names to IDs
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
    for repo_name in select_highlighted_repos_asset:
        repo_id = repo_by_name.get(repo_name)
        if repo_id:
            generate_repo_blurb(repo_id)


@asset(partitions_def=users_partitions)
def finalize_user_profile_asset(
    context: AssetExecutionContext,
    infer_user_theme_asset: str,
    select_highlighted_repos_asset: List[str],
) -> None:
    """Finalize the user profile with theme and highlighted repos."""
    username = context.partition_key
    finalize_user_profile(username)
