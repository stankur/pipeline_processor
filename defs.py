"""
Dagster definitions for the worker pipeline.
"""
from dagster import Definitions, AssetGraph

from assets import (
    fetch_profile_asset,
    fetch_repos_asset,
    select_highlighted_repos_asset,
    infer_user_theme_asset,
    enhance_repo_media_asset,
    generate_repo_blurb_asset,
    extract_repo_emphasis_asset,
    extract_repo_keywords_asset,
    extract_repo_kind_asset,
)

# All assets already have partitions defined
all_assets = [
    fetch_profile_asset,
    fetch_repos_asset,
    select_highlighted_repos_asset,
    infer_user_theme_asset,
    enhance_repo_media_asset,
    generate_repo_blurb_asset,
    extract_repo_emphasis_asset,
    extract_repo_keywords_asset,
    extract_repo_kind_asset,
]

defs = Definitions(
    assets=all_assets,
)

# Export graph and asset metadata for programmatic restarts
asset_graph = AssetGraph.from_assets(all_assets)
asset_meta = {
    a.key.to_user_string(): {
        "kinds": list(a.metadata.get("work_item_kinds", [])),
        "scope": a.metadata.get("scope"),
        "key": a.key,
    }
    for a in all_assets
}
