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
    list_user_work_items,
    reset_user_work_items_to_pending,
    reset_user_repo_work_items_to_pending,
    delete_user_repo_subjects,
    delete_user_completely,
    upsert_subject,
    upsert_user_repo_link,
    get_user_subject,
    get_repo_subject,
    upsert_user_subject,
    upsert_repo_subject,
)
from defs import all_assets, asset_meta, defs as dag_defs
from models import UserSubject, RepoSubject, GalleryImage


app = Flask(__name__)

# CORS: allow only configured origins (comma-separated). Example local: http://localhost:3000
_origins = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()]
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
        print(f"[api] materializing assets for username={username} selection={selection}")

        run_config = _build_run_config_for_username(username)

        result = materialize(
            assets=all_assets,
            run_config=run_config,
            selection=selection,
            raise_on_error=False,  # Don't crash API on task failures
        )

        print(f"[api] materialization completed for username={username} success={result.success}")
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
    print(f"[DEBUG] API_KEY present: {bool(api_key)}, len={len(api_key) if api_key else 0}")
    if not api_key:
        return None  # no auth enforced
    # Allow unauthenticated health endpoint
    if request.path in ("/healthz", "/_healthz"):
        return None
    auth = request.headers.get("Authorization", "")
    print(f"[DEBUG] Auth header present: {bool(auth)}, starts_with_Bearer: {auth.startswith('Bearer ')}")
    if auth.startswith("Bearer "):
        provided_key = auth.split(" ", 1)[1].strip()
        print(f"[DEBUG] Provided key len={len(provided_key)}, expected len={len(api_key)}, match={provided_key == api_key}")
        print(f"[DEBUG] Expected key: {api_key}")
        print(f"[DEBUG] Provided key: {provided_key}")
        if provided_key != api_key:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
    else:
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
        return jsonify({
            "ok": True,
            "status": "new",
            "processing": True,
            "data_ready": False,
        })
    
    if user.is_ghost:
        print(f"[api] login: activating ghost username={username}")
        user.is_ghost = False
        upsert_user_subject(conn, username, user)
        conn.commit()
        return jsonify({
            "ok": True,
            "status": "activated",
            "processing": False,
            "data_ready": True,
        })
    
    print(f"[api] login: existing user username={username}")
    return jsonify({
        "ok": True,
        "status": "existing",
        "processing": False,
        "data_ready": True,
    })


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
        return jsonify({
            "ok": False,
            "error": "user_exists",
            "message": "User already exists. Use ?force=true to recreate.",
            "is_ghost": existing.is_ghost,
        }), 400
    
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
    
    return jsonify({
        "ok": True,
        "created": True,
        "is_ghost": True,
        "processing": True,
    })


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
        ],
    )

    # Reset repo-scoped work items for repos linked to this user
    reset_user_repo_work_items_to_pending(
        conn,
        username,
        [
            "enhance_repo_media",
            "generate_repo_blurb",
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


@app.delete("/users/<username>")
def delete_user(username: str):
    """Delete a user and all their associated resources.
    
    Removes user subject, work items, repo links, and owned repos.
    """
    print(f"[api] delete_user called username={username}")
    conn = get_conn()
    
    # Check if user exists
    user = get_user_subject(conn, username)
    if not user:
        return jsonify({"ok": False, "error": "user_not_found"}), 404
    
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
    """Get user and repo data."""
    conn = get_conn()
    u = get_subject(conn, "user", username)
    repos = get_user_repos(conn, username)
    
    user_data = None
    if u and u["data_json"]:
        try:
            user_model = UserSubject.model_validate_json(u["data_json"])
            user_data = user_model.model_dump()
        except Exception:
            user_data = None
    
    repos_data = []
    for r in repos:
        if r["data_json"]:
            try:
                repo_model = RepoSubject.model_validate_json(r["data_json"])
                repos_data.append(repo_model.model_dump())
            except Exception:
                continue
    
    return jsonify({"user": user_data, "repos": repos_data})


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
                    "output_json": json.loads(it["output_json"]) if it["output_json"] else None,
                }
                for it in items
            ]
        }
    )


@app.get("/for-you/<viewer_username>")
def for_you(viewer_username: str):
    """Return a simple For You feed built from user-subject highlights.

    Implementation details (mock, non-personalized):
      - Read ONLY user subjects where data_json.highlighted_repos is a non-empty list.
      - For each such user, resolve highlighted names to that user's repos via get_user_repos.
      - If multiple repos share the same name for a user, select by:
          1) highest user_commit_days (None treated as -1)
          2) if tied, prefer external original (owner != username)
          3) if still tied, newest subjects.updated_at
      - Flatten across users and sort by subjects.updated_at desc.
      - Each repo object is EXACTLY the stored repo subject JSON, plus an added "username" field.
    """
    conn = get_conn()

    # Discover users with highlighted_repos on their user subject
    user_rows = conn.execute(
        "SELECT subject_id, data_json FROM subjects WHERE subject_type='user'"
    ).fetchall()

    user_to_highlight_names: dict[str, list[str]] = {}
    user_to_is_ghost: dict[str, bool] = {}
    for row in user_rows:
        if not row.get("data_json"):
            continue
        try:
            user = UserSubject.model_validate_json(row["data_json"])
            if user.highlighted_repos:
                username = str(row.get("subject_id") or "").strip()
                if username:
                    user_to_highlight_names[username] = user.highlighted_repos
                    user_to_is_ghost[username] = user.is_ghost
        except Exception:
            continue

    # Build feed
    feed: list[tuple[float, dict]] = []  # (subjects.updated_at, repo_json_with_username)

    for username, names in user_to_highlight_names.items():
        repo_rows = get_user_repos(conn, username)

        # Index candidate repos by bare name
        name_to_candidates: dict[str, list[dict]] = {}
        for r in repo_rows:
            data_json = r.get("data_json")
            if not data_json:
                continue
            try:
                repo = RepoSubject.model_validate_json(data_json)
                # Stash parsed object alongside the row for selection
                name_to_candidates.setdefault(repo.name, []).append({
                    "obj": repo.model_dump(),
                    "row": r,
                })
            except Exception:
                continue

        for repo_name in names:
            candidates = name_to_candidates.get(repo_name)
            if not candidates:
                continue

            def _selection_key(cand: dict) -> tuple:
                r = cand["row"]
                subj_id = r.get("subject_id") or ""
                owner = subj_id.split("/", 1)[0] if "/" in subj_id else ""
                # Primary: user_commit_days (None -> -1)
                days = r.get("user_commit_days")
                days_val = days if isinstance(days, int) else -1
                # Secondary: prefer external original (owner != username) -> rank 1,
                #            owned (owner == username) -> rank 0; we want external preferred, so external=1.
                external_flag = 1 if owner != username else 0
                # Tertiary: newer updated_at
                upd = r.get("updated_at") or 0
                return (days_val, external_flag, upd)

            best = max(candidates, key=_selection_key)
            obj = dict(best["obj"])  # copy to avoid mutating cached
            obj["username"] = username
            obj["is_ghost"] = user_to_is_ghost.get(username, False)
            upd = best["row"].get("updated_at") or 0
            feed.append((upd, obj))

    # Sort by subjects.updated_at descending
    feed.sort(key=lambda t: t[0], reverse=True)

    # Deduplicate by canonical repo id (owner/repo), keep the first (most recent)
    seen: set[str] = set()
    repos: list[dict] = []
    for _, obj in feed:
        rid = obj.get("id") if isinstance(obj, dict) else None
        if not isinstance(rid, str) or not rid:
            continue
        if rid in seen:
            continue
        seen.add(rid)
        repos.append(obj)
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
        is_highlight = bool(img.get("is_highlight")) if isinstance(img.get("is_highlight"), (bool, int)) else False

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

        to_add.append({
            "url": url,
            "alt": (img.get("alt") or "").strip(),
            "original_url": (img.get("original_url") or "").strip() or url,
            "title": title,
            "caption": caption,
            "is_highlight": is_highlight,
            "taken_at": taken_at,
        })

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
        key_val = item.get(dedupe_key, "").strip() if isinstance(dedupe_key, str) else None
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

    return jsonify({
        "ok": True,
        "repo_id": repo_id,
        "added": added,
        "skipped": skipped,
        "gallery_count": len(repo.gallery),
    })


@app.delete("/users/<username>/repos/<owner>/<repo>/gallery")
def delete_repo_gallery_images(username: str, owner: str, repo: str):
    """Remove images from a repo's gallery by URL.

    Accepts either query params (?url=...&url=...) or JSON body {"urls": [..]} or {"url": "..."}.
    """
    repo_id = f"{owner}/{repo}"

    urls = set([u.strip() for u in request.args.getlist("url") if isinstance(u, str) and u.strip()])
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

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)


