import json
from datetime import datetime, timezone
import os
import threading
from flask import Flask, jsonify, request
from flask_cors import CORS
from dagster import materialize, AssetSelection

from db import (
    get_conn,
    init_db,
    get_subject,
    get_user_repos,
    get_work_item,
    list_user_work_items,
    reset_user_work_items_to_pending,
    reset_user_repo_work_items_to_pending,
    delete_user_repo_subjects,
    delete_user_completely,
    delete_user_recommendations,
    upsert_subject,
    upsert_user_repo_link,
    get_user_subject,
    get_repo_subject,
    upsert_user_subject,
    upsert_repo_subject,
    create_user_context,
    get_user_contexts,
    delete_user_context,
)
from defs import all_assets, asset_meta, defs as dag_defs
from models import UserSubject, RepoSubject, GalleryImage, ForYouRepoItem
from feed.rank import build_feed_for_user, build_user_feed, build_hackernews_feed
from gallery import get_gallery_repos
from tasks import embed_user_context


app = Flask(__name__)

# CORS: allow only configured origins (comma-separated). Example local: http://localhost:3000
_origins = [
    o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()
]
if _origins:
    CORS(app, resources={r"/*": {"origins": _origins}})
else:
    # If not set, default to no CORS restrictions (safest is to set ALLOWED_ORIGINS in prod)
    CORS(app)

# Initialize DB at import time
init_db()


def _build_run_config_for_username(username: str) -> dict:
    """Construct Dagster run_config with per-asset config entries.

    Each asset defined in `all_assets` expects a `UserConfig` with `username`.
    Dagster requires config under root:ops, keyed by op/asset name.
    """
    ops_config: dict[str, dict] = {}
    for asset_def in all_assets:
        # Use the asset key as the op name (matches Dagster error expectation)
        op_name = asset_def.key.to_user_string()
        ops_config[op_name] = {"config": {"username": username}}
    return {"ops": ops_config}


def _run_worker(username: str, selection: list[str] | None = None):
    """Run the Dagster worker for a specific username using SDK."""
    try:
        print(
            f"[api] materializing assets for username={username} selection={selection}"
        )

        run_config = _build_run_config_for_username(username)

        result = materialize(
            assets=all_assets,
            run_config=run_config,
            selection=selection,
            raise_on_error=False,  # Don't crash API on task failures
        )

        print(
            f"[api] materialization completed for username={username} success={result.success}"
        )
        return result.success

    except Exception as e:
        print(f"[api] materialization error for username={username}: {e}")
        return False


def _run_worker_async(username: str, selection: list[str] | None = None):
    """Run worker in background thread to avoid blocking API."""
    thread = threading.Thread(target=_run_worker, args=(username, selection))
    thread.daemon = True
    thread.start()


@app.before_request
def _require_api_key():
    """Require API key for all routes except health checks when API_KEY is set.

    Expect header: Authorization: Bearer <API_KEY>
    """
    api_key = (os.environ.get("API_KEY") or "").strip()
    if not api_key:
        return None  # no auth enforced
    # Allow unauthenticated health endpoint
    if request.path in ("/healthz", "/_healthz"):
        return None
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth.split(" ", 1)[1].strip() != api_key:
        return jsonify({"ok": False, "error": "unauthorized"}), 401


@app.post("/users/<username>/login")
def login(username: str):
    """Handle user login and determine if pipeline needs to run.

    Returns status: "new" | "activated" | "existing"
    """
    print(f"[api] login called username={username}")
    conn = get_conn()

    user = get_user_subject(conn, username)

    if not user:
        print(f"[api] login: new user username={username}")
        _run_worker_async(username)
        return jsonify(
            {
                "ok": True,
                "status": "new",
                "processing": True,
                "data_ready": False,
            }
        )

    if user.is_ghost:
        print(f"[api] login: activating ghost username={username}")
        user.is_ghost = False
        upsert_user_subject(conn, username, user)
        conn.commit()
        _run_worker_async(username)
        return jsonify(
            {
                "ok": True,
                "status": "activated",
                "processing": False,
                "data_ready": True,
            }
        )

    print(f"[api] login: existing user username={username}")
    _run_worker_async(username)
    return jsonify(
        {
            "ok": True,
            "status": "existing",
            "processing": False,
            "data_ready": True,
        }
    )


@app.post("/ghost-users/<username>")
def create_ghost_user(username: str):
    """Create an inactive user account with their data pre-fetched.

    Query params:
      - force: If true, recreate even if user exists
    """
    print(f"[api] create_ghost_user called username={username}")
    force = request.args.get("force", "").lower() in ("true", "1", "yes")

    conn = get_conn()
    existing = get_user_subject(conn, username)

    if existing and not force:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "user_exists",
                    "message": "User already exists. Use ?force=true to recreate.",
                    "is_ghost": existing.is_ghost,
                }
            ),
            400,
        )

    if existing and force:
        print(f"[api] create_ghost_user: forcing recreation username={username}")
        existing.is_ghost = True
        upsert_user_subject(conn, username, existing)
        conn.commit()
    else:
        print(f"[api] create_ghost_user: creating new ghost username={username}")
        ghost = UserSubject(login=username, is_ghost=True)
        upsert_user_subject(conn, username, ghost)
        conn.commit()

    _run_worker_async(username)

    return jsonify(
        {
            "ok": True,
            "created": True,
            "is_ghost": True,
            "processing": True,
        }
    )


@app.post("/users/<username>/start")
def start(username: str):
    """Start the full pipeline for a user."""
    print(f"[api] start called username={username}")
    _run_worker_async(username)
    print(f"[api] start queued username={username}")
    return jsonify({"ok": True, "queued": True})


@app.post("/users/<username>/restart")
def restart(username: str):
    """Force reset and rerun the full pipeline for a user.

    Clears user-owned repos and resets work items (user + repo scoped) to pending so tasks will rerun even with idempotent guards.
    """
    print(f"[api] restart called username={username}")
    conn = get_conn()

    # Reset user-scoped work items
    reset_user_work_items_to_pending(
        conn,
        username,
        [
            "fetch_profile",
            "fetch_repos",
            "select_highlighted_repos",
            "infer_user_theme",
            "embed_user_profile",
        ],
    )

    # Reset repo-scoped work items for repos linked to this user
    reset_user_repo_work_items_to_pending(
        conn,
        username,
        [
            "enhance_repo_media",
            "generate_repo_blurb",
            "embed_repo",
            "extract_repo_emphasis",
            "extract_repo_keywords",
            "extract_repo_kind",
        ],
    )

    # Remove links and owned repo subjects (ensures fresh re-discovery)
    delete_user_repo_subjects(conn, username)

    conn.commit()

    # Queue run
    _run_worker_async(username)
    print(f"[api] restart queued username={username}")
    return jsonify({"ok": True, "queued": True, "forced": True})


@app.post("/users/<username>/clear-recommendations")
def clear_recommendations(username: str):
    """Clear all cached recommendations for a user.

    Deletes all feed recommendation judgments, forcing fresh LLM evaluation on next build.
    Useful when prompt logic changes or for testing from clean state.
    Does NOT affect user profile, repos, or work items.

    Works even if user doesn't exist in subjects table (clears orphaned data).
    """
    print(f"[api] clear_recommendations called username={username}")
    conn = get_conn()

    delete_user_recommendations(conn, username)
    conn.commit()

    print(f"[api] clear_recommendations completed username={username}")
    return jsonify({"ok": True, "cleared": True})


@app.delete("/users/<username>")
def delete_user(username: str):
    """Delete a user and all their associated resources.

    Removes user subject, work items, repo links, and owned repos.
    Works even if user doesn't exist (cleans up any orphaned data).
    """
    print(f"[api] delete_user called username={username}")
    conn = get_conn()

    delete_user_completely(conn, username)
    conn.commit()

    print(f"[api] delete_user completed username={username}")
    return jsonify({"ok": True, "deleted": True})


@app.post("/users/<username>/restart-from")
def restart_from(username: str):
    """Restart from a given asset key and downstream only, resetting just the relevant work-item kinds.

    Query params:
      - start: asset key string (e.g., 'generate_repo_blurb_asset')
    """
    start = (request.args.get("start") or "").strip()
    if not start:
        return jsonify({"ok": False, "error": "missing_start"}), 400
    meta = asset_meta.get(start)
    if not meta:
        return jsonify({"ok": False, "error": "invalid_start"}), 400

    # Compute downstream selection including start
    selection = AssetSelection.keys(meta["key"]).downstream()
    selected_keys = selection.resolve(dag_defs)
    selected_names = [k.to_user_string() for k in selected_keys]

    # Collect kinds to reset grouped by scope
    user_kinds: set[str] = set()
    repo_kinds: set[str] = set()
    for name in selected_names:
        m = asset_meta.get(name) or {}
        kinds = m.get("kinds") or []
        scope = m.get("scope")
        if scope == "user":
            user_kinds.update(kinds)
        elif scope == "repo":
            repo_kinds.update(kinds)

    # Reset and materialize
    conn = get_conn()
    if user_kinds:
        reset_user_work_items_to_pending(conn, username, sorted(user_kinds))
    if repo_kinds:
        reset_user_repo_work_items_to_pending(conn, username, sorted(repo_kinds))
    conn.commit()

    run_config = _build_run_config_for_username(username)
    result = materialize(
        assets=all_assets,
        run_config=run_config,
        selection=selection,
        raise_on_error=False,
    )
    return jsonify(
        {
            "ok": True,
            "success": result.success,
            "reset": {
                "user_kinds": sorted(user_kinds),
                "repo_kinds": sorted(repo_kinds),
            },
            "selection": selected_names,
        }
    )


@app.get("/users/<username>/data")
def data(username: str):
    """Get user and repo data.

    Returns:
        - user: user profile data or null
        - repos: list of repos if fetch_repos succeeded, null if not yet fetched
    """
    conn = get_conn()
    u = get_subject(conn, "user", username)

    user_data = None
    if u and u["data_json"]:
        try:
            user_model = UserSubject.model_validate_json(u["data_json"])
            user_data = user_model.model_dump()
        except Exception:
            user_data = None

    # Check if fetch_repos task has completed
    fetch_repos_work = get_work_item(conn, "fetch_repos", "user", username)
    repos_fetched = fetch_repos_work and fetch_repos_work.status == "succeeded"

    repos_data = None
    if repos_fetched:
        # Repos have been fetched - include the list (even if empty)
        repos = get_user_repos(conn, username)
        repos_data = []
        for r in repos:
            if r["data_json"]:
                try:
                    repo_model = RepoSubject.model_validate_json(r["data_json"])
                    repos_data.append(repo_model.model_dump())
                except Exception:
                    continue

    return jsonify({"user": user_data, "repos": repos_data})


@app.get("/users/<username>/activity")
def get_activity(username: str):
    """Get user's daily coding activity (repos and languages touched per day).
    
    Returns presence-only data (no commit counts) for building heatmaps.
    Data covers the last 2 years by default.
    
    Returns:
        - ok: bool
        - activity: dict with:
            - version: int (schema version)
            - window_years: int (time window in years)
            - days: dict of {date: {repos: [...], languages: [...]}}
            - repo_days: dict of {repo_id: [dates...]}
            - language_days: dict of {language: [dates...]}
        - null if not yet collected
    """
    conn = get_conn()
    row = get_subject(conn, "user_activity", username)
    
    activity_data = None
    if row and row.get("data_json"):
        try:
            activity_data = json.loads(row["data_json"])
        except Exception:
            activity_data = None
    
    return jsonify({"ok": True, "activity": activity_data})


@app.get("/users/<username>/progress")
def progress(username: str):
    """Get work item progress for a user."""
    conn = get_conn()
    items = list_user_work_items(conn, username)
    return jsonify(
        {
            "items": [
                {
                    "kind": it["kind"],
                    "status": it["status"],
                    "processed_at": it["processed_at"],
                    "output_json": json.loads(it["output_json"])
                    if it["output_json"]
                    else None,
                }
                for it in items
            ]
        }
    )


@app.get("/for-you/<viewer_username>")
def for_you(viewer_username: str):
    """Community For You feed using embedding similarity.

    Query params:
      - limit: max items to return (default 30)

    Returns personalized feed using embedding-based similarity ranking with fatigue penalty.
    Fast - no LLM calls, no cache needed.
    """
    conn = get_conn()
    limit = int((request.args.get("limit") or "30").strip() or "30")
    items = build_feed_for_user(conn, viewer_username, source="community", limit=limit)
    return jsonify({"repos": [item.model_dump() for item in items]})


@app.get("/for-you-trending/<viewer_username>")
def for_you_trending(viewer_username: str):
    """Fast trending feed from cached recommendations.

    Query params:
      - limit: max items to return (default 30)

    Returns personalized trending feed using cached LLM judgments and fatigue ranking.
    Use POST to rebuild/populate cache.
    """
    conn = get_conn()
    limit = int((request.args.get("limit") or "30").strip() or "30")
    items = build_feed_for_user(conn, viewer_username, source="trending", limit=limit)
    return jsonify({"repos": [item.model_dump() for item in items]})


@app.post("/for-you-trending/<viewer_username>")
def build_for_you_trending(viewer_username: str):
    """Build trending feed by fetching/judging trending repos (slow, populates cache).

    Query params:
      - limit: max items to evaluate (default 30)

    Fetches trending repos from GitHub (if stale), filters by user's languages,
    runs LLM judgments to populate cache. Returns top ranked items.
    """
    conn = get_conn()
    limit = int((request.args.get("limit") or "30").strip() or "30")
    items = build_feed_for_user(conn, viewer_username, source="trending", limit=limit)
    return jsonify(
        {
            "status": "completed",
            "judged": len(items),
            "repos": [item.model_dump() for item in items],
        }
    )


@app.get("/for-you-users/<viewer_username>")
def for_you_users(viewer_username: str):
    """User recommendations feed using embedding similarity.

    Query params:
      - limit: max users to return (default 30)

    Returns list of similar users based on profile embeddings.
    No fatigue penalty - simple similarity ranking.
    """
    conn = get_conn()
    limit = int((request.args.get("limit") or "30").strip() or "30")
    items = build_user_feed(conn, viewer_username, limit=limit)
    return jsonify({"users": [item.model_dump() for item in items]})


@app.get("/for-you-hackernews/<viewer_username>")
def for_you_hackernews(viewer_username: str):
    """HN story recommendations using embedding similarity.

    Query params:
      - limit: max stories to return (default 30)

    Returns personalized HN feed with fatigue penalty.
    Automatically refreshes stories if cache is stale (>6 hours).
    """
    conn = get_conn()
    limit = int((request.args.get("limit") or "30").strip() or "30")
    items = build_hackernews_feed(conn, viewer_username, limit=limit)
    return jsonify({"stories": [item.model_dump() for item in items]})


@app.get("/gallery")
def get_gallery():
    """Get global gallery of highlighted repos with images.

    Query params:
      - limit: max items to return (default 30)

    Returns highlighted repos with gallery images, sorted by recency.
    No per-user filtering - this is a global gallery visible to all.
    """
    conn = get_conn()
    limit_str = request.args.get("limit", "30")
    try:
        limit = int(limit_str)
    except (ValueError, TypeError):
        limit = 30
    repos = get_gallery_repos(conn, limit=limit)
    return jsonify({"repos": repos})


@app.post("/users/<username>/repos/<owner>/<repo>/gallery")
def add_repo_gallery_images(username: str, owner: str, repo: str):
    """Append one or more images to a repo's gallery, creating the subject if missing.

    Body can be either a single image object or a batch:
    - {"url": str, "alt": str, "original_url": str}
    - {"images": [{...}], "dedupe": "url" | false, "link": bool}
    """
    repo_id = f"{owner}/{repo}"
    data = request.get_json(silent=True) or {}

    # Normalize input to a list of images
    images = []
    if isinstance(data.get("images"), list):
        images = [img for img in data.get("images") if isinstance(img, dict)]
    elif any(k in data for k in ("url", "alt", "original_url")):
        images = [data]
    else:
        return jsonify({"ok": False, "error": "invalid_body"}), 400

    # Build list to add with minimal validation
    to_add = []
    for img in images:
        url = (img.get("url") or "").strip()
        if not url:
            continue
        # Optional fields with defaults
        title = (img.get("title") or "").strip()
        caption = (img.get("caption") or "").strip()
        is_highlight = (
            bool(img.get("is_highlight"))
            if isinstance(img.get("is_highlight"), (bool, int))
            else False
        )

        # taken_at as epoch milliseconds (number). Accept numeric, or parse ISO/date-like string; default to now.
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        taken_at_val = img.get("taken_at")
        taken_at: int
        if isinstance(taken_at_val, (int, float)):
            try:
                taken_at = int(taken_at_val)
            except Exception:
                taken_at = now_ms
        elif isinstance(taken_at_val, str) and taken_at_val.strip():
            s = taken_at_val.strip()
            try:
                # Try numeric string first
                if s.isdigit():
                    taken_at = int(s)
                else:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    taken_at = int(dt.timestamp() * 1000)
            except Exception:
                taken_at = now_ms
        else:
            taken_at = now_ms

        to_add.append(
            {
                "url": url,
                "alt": (img.get("alt") or "").strip(),
                "original_url": (img.get("original_url") or "").strip() or url,
                "title": title,
                "caption": caption,
                "is_highlight": is_highlight,
                "taken_at": taken_at,
            }
        )

    if not to_add:
        return jsonify({"ok": False, "error": "no_images"}), 400

    dedupe_key = data.get("dedupe", "url")
    link_flag = bool(data.get("link"))

    conn = get_conn()

    # Load existing repo subject (or initialize)
    repo = get_repo_subject(conn, repo_id)
    if not repo:
        repo = RepoSubject(id=repo_id, name=repo)

    # Build existing keys set for dedupe
    existing_keys = set()
    if dedupe_key is not False and dedupe_key in ["url", "original_url"]:
        for img in repo.gallery:
            val = getattr(img, dedupe_key, None)
            if val:
                existing_keys.add(val.strip())

    added = 0
    skipped = 0
    for item in to_add:
        key_val = (
            item.get(dedupe_key, "").strip() if isinstance(dedupe_key, str) else None
        )
        if dedupe_key is not False and key_val in existing_keys:
            skipped += 1
            continue
        try:
            gallery_img = GalleryImage(**item)
            repo.gallery.append(gallery_img)
            if dedupe_key is not False and isinstance(key_val, str):
                existing_keys.add(key_val)
            added += 1
        except Exception:
            skipped += 1
            continue

    # Persist subject
    upsert_repo_subject(conn, repo_id, repo)

    # Optionally ensure link for user->repo
    if link_flag:
        try:
            upsert_user_repo_link(conn, username, repo_id, "manual", None)
        except Exception:
            pass

    conn.commit()

    return jsonify(
        {
            "ok": True,
            "repo_id": repo_id,
            "added": added,
            "skipped": skipped,
            "gallery_count": len(repo.gallery),
        }
    )


@app.delete("/users/<username>/repos/<owner>/<repo>/gallery")
def delete_repo_gallery_images(username: str, owner: str, repo: str):
    """Remove images from a repo's gallery by URL.

    Accepts either query params (?url=...&url=...) or JSON body {"urls": [..]} or {"url": "..."}.
    """
    repo_id = f"{owner}/{repo}"

    urls = set(
        [
            u.strip()
            for u in request.args.getlist("url")
            if isinstance(u, str) and u.strip()
        ]
    )
    if not urls:
        body = request.get_json(silent=True) or {}
        if isinstance(body.get("urls"), list):
            urls = set([str(u).strip() for u in body.get("urls") if str(u).strip()])
        elif isinstance(body.get("url"), str) and body.get("url").strip():
            urls = {body.get("url").strip()}

    if not urls:
        return jsonify({"ok": False, "error": "no_urls"}), 400

    conn = get_conn()
    repo = get_repo_subject(conn, repo_id)

    if not repo or not repo.gallery:
        # Nothing to remove
        return jsonify({"ok": True, "removed": 0, "gallery_count": 0})

    new_gallery = []
    removed = 0
    for img in repo.gallery:
        if img.url in urls:
            removed += 1
            continue
        new_gallery.append(img)

    if removed > 0:
        repo.gallery = new_gallery
        upsert_repo_subject(conn, repo_id, repo)
        conn.commit()

    return jsonify({"ok": True, "removed": removed, "gallery_count": len(repo.gallery)})


@app.get("/users/<username>/repos/<owner>/<repo>/gallery")
def get_repo_gallery(username: str, owner: str, repo: str):
    """Get all images in a repository's gallery.

    Returns the gallery as an array of image objects.
    """
    repo_id = f"{owner}/{repo}"
    conn = get_conn()
    repo_obj = get_repo_subject(conn, repo_id)

    if not repo_obj:
        return jsonify({"gallery": []})

    gallery = [img.model_dump() for img in repo_obj.gallery]
    return jsonify({"gallery": gallery})


@app.patch("/users/<username>/repos/<owner>/<repo>/gallery")
def update_repo_gallery_image(username: str, owner: str, repo: str):
    """Update fields of a single gallery image identified by URL.

    Body:
      - {"url": string, ...fields}

    Updatable fields: title, caption, is_highlight, alt, original_url, taken_at
    Returns: { ok: bool, updated: int }
    """
    repo_id = f"{owner}/{repo}"
    body = request.get_json(silent=True) or {}
    url = (body.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "missing_url"}), 400

    # Normalize and validate updatable fields
    updatable = {}
    if "title" in body:
        updatable["title"] = (body.get("title") or "").strip()
    if "caption" in body:
        updatable["caption"] = (body.get("caption") or "").strip()
    if "alt" in body:
        updatable["alt"] = (body.get("alt") or "").strip()
    if "original_url" in body:
        updatable["original_url"] = (body.get("original_url") or "").strip()
    if "is_highlight" in body:
        ih = body.get("is_highlight")
        if isinstance(ih, (bool, int)):
            updatable["is_highlight"] = bool(ih)
    if "taken_at" in body:
        t = body.get("taken_at")
        if isinstance(t, (int, float)):
            try:
                updatable["taken_at"] = int(t)
            except Exception:
                pass
        elif isinstance(t, str) and t.strip():
            s = t.strip()
            try:
                if s.isdigit():
                    updatable["taken_at"] = int(s)
                else:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    updatable["taken_at"] = int(dt.timestamp() * 1000)
            except Exception:
                pass

    if not updatable:
        return jsonify({"ok": False, "error": "no_fields"}), 400

    conn = get_conn()
    repo_obj = get_repo_subject(conn, repo_id)

    if not repo_obj or not repo_obj.gallery:
        return jsonify({"ok": True, "updated": 0})

    updated = 0
    for img in repo_obj.gallery:
        if img.url == url:
            # Update whitelisted fields only
            for k, v in updatable.items():
                setattr(img, k, v)
            updated += 1
            break

    if updated:
        try:
            upsert_repo_subject(conn, repo_id, repo_obj)
            conn.commit()
        except Exception:
            pass

    return jsonify({"ok": True, "updated": updated})


@app.post("/contexts/<username>")
def create_context(username: str):
    """Create a new context for a user and trigger embedding.
    
    Path params:
        username: GitHub username
    
    Body params (form data):
        content: Context text content
    
    Returns:
        {"context_id": "username:uuid", "status": "created"}
    """
    content = request.form.get("content")
    
    if not content or not content.strip():
        return jsonify({"error": "Content cannot be empty"}), 400
    
    conn = get_conn()
    
    try:
        # Create context
        context = create_user_context(conn, username, content.strip())
        conn.commit()
        
        # Trigger embedding (synchronous for now)
        embed_user_context(context.id)
        
        return jsonify({
            "context_id": context.id,
            "status": "created",
            "username": username
        })
    except Exception as e:
        print(f"[api] create_context ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.get("/contexts/<username>")
def list_contexts(username: str):
    """Get all contexts for a user.
    
    Returns:
        {"contexts": [{"id": "...", "content": "...", "created_at": ..., "has_embedding": bool}]}
    """
    conn = get_conn()
    
    try:
        contexts = get_user_contexts(conn, username)
        
        # Format response (exclude embedding vector, just indicate if exists)
        formatted = [
            {
                "id": ctx.id,
                "content": ctx.content,
                "created_at": ctx.created_at,
                "has_embedding": ctx.embedding is not None
            }
            for ctx in contexts
        ]
        
        return jsonify({"contexts": formatted, "username": username})
    except Exception as e:
        print(f"[api] list_contexts ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.delete("/contexts/<context_id>")
def delete_context_endpoint(context_id: str):
    """Delete a user context.
    
    Returns:
        {"status": "deleted"} or {"error": "not_found"}
    """
    conn = get_conn()
    
    try:
        deleted = delete_user_context(conn, context_id)
        conn.commit()
        
        if deleted:
            return jsonify({"status": "deleted", "context_id": context_id})
        else:
            return jsonify({"error": "not_found"}), 404
    except Exception as e:
        print(f"[api] delete_context ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
