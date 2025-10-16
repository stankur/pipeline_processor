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
    embed_repo_asset,
    embed_user_profile_asset,
    extract_repo_emphasis_asset,
    extract_repo_keywords_asset,
    extract_repo_kind_asset,
    build_for_you_community_asset,
    build_for_you_trending_asset,
)

# All assets already have partitions defined
all_assets = [
    fetch_profile_asset,
    fetch_repos_asset,
    select_highlighted_repos_asset,
    infer_user_theme_asset,
    enhance_repo_media_asset,
    generate_repo_blurb_asset,
    embed_repo_asset,
    embed_user_profile_asset,
    extract_repo_emphasis_asset,
    extract_repo_keywords_asset,
    extract_repo_kind_asset,
    build_for_you_community_asset,
    build_for_you_trending_asset,
]

defs = Definitions(
    assets=all_assets,
)

# Export asset metadata for programmatic restarts (no AssetGraph dependency)
# Use metadata_by_key since AssetsDefinition has no direct `metadata` attribute
asset_meta = {
    a.key.to_user_string(): {
        "kinds": list((a.metadata_by_key.get(a.key) or {}).get("work_item_kinds", [])),
        "scope": (a.metadata_by_key.get(a.key) or {}).get("scope"),
        "key": a.key,
    }
    for a in all_assets
}
