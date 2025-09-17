"""
Dagster definitions for the worker pipeline.
"""
from dagster import Definitions

from assets import (
    fetch_profile_asset,
    fetch_repos_asset,
    select_highlighted_repos_asset,
    infer_user_theme_asset,
    enhance_repo_media_asset,
    generate_repo_blurb_asset,
    finalize_user_profile_asset,
)

# All assets already have partitions defined
all_assets = [
    fetch_profile_asset,
    fetch_repos_asset,
    select_highlighted_repos_asset,
    infer_user_theme_asset,
    enhance_repo_media_asset,
    generate_repo_blurb_asset,
    finalize_user_profile_asset,
]

defs = Definitions(
    assets=all_assets,
)
