import json
import os
import threading
from flask import Flask, jsonify, request
from flask_cors import CORS
from dagster import materialize

from db import (
    get_conn,
    init_db,
    get_subject,
    get_user_repos,
    list_user_work_items,
    reset_user_work_items_to_pending,
    reset_user_repo_work_items_to_pending,
    delete_user_repo_subjects,
)
from defs import all_assets


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
    if not api_key:
        return None  # no auth enforced
    # Allow unauthenticated health endpoint
    if request.path in ("/healthz", "/_healthz"):
        return None
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth.split(" ", 1)[1].strip() != api_key:
        return jsonify({"ok": False, "error": "unauthorized"}), 401


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
        ],
    )

    # Remove links and owned repo subjects (ensures fresh re-discovery)
    delete_user_repo_subjects(conn, username)

    conn.commit()

    # Queue run
    _run_worker_async(username)
    print(f"[api] restart queued username={username}")
    return jsonify({"ok": True, "queued": True, "forced": True})


@app.get("/users/<username>/data")
def data(username: str):
    """Get user and repo data."""
    conn = get_conn()
    u = get_subject(conn, "user", username)
    repos = get_user_repos(conn, username)
    return jsonify(
        {
            "user": json.loads(u["data_json"]) if u and u["data_json"] else None,
            "repos": [json.loads(r["data_json"]) for r in repos if r["data_json"]],
        }
    )


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


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)


