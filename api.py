import json
import threading
from flask import Flask, jsonify, request
from dagster import materialize

from db import (
    get_conn, 
    init_db, 
    get_subject, 
    get_user_repos, 
    list_user_work_items, 
    reset_user_work_items_to_pending, 
    delete_user_repo_subjects
)
from defs import all_assets

app = Flask(__name__)

# Initialize DB at import time
init_db()


def _run_worker(username: str, selection: list[str] | None = None):
    """Run the Dagster worker for a specific username using SDK."""
    try:
        print(f"[api] materializing assets for username={username} selection={selection}")
        
        # Use Dagster SDK to materialize assets
        result = materialize(
            assets=all_assets,
            partition_key=username,
            selection=selection,
            raise_on_error=False  # Don't crash API on task failures
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


@app.post("/users/<username>/start")
def start(username: str):
    """Start the full pipeline for a user."""
    print(f"[api] start called username={username}")
    _run_worker(username)
    print(f"[api] start done username={username}")
    return jsonify({"ok": True, "queued": True})


@app.post("/users/<username>/restart")
def restart(username: str):
    """Force reset and rerun the full pipeline for a user."""
    print(f"[api] restart called username={username}")
    conn = get_conn()
    
    # Clean up existing data
    delete_user_repo_subjects(conn, username)
    reset_user_work_items_to_pending(conn, username, [
        "fetch_profile", 
        "fetch_repos", 
        "select_highlighted_repos",
        "infer_user_theme",
        "enhance_repo_media",
        "generate_repo_blurb", 
        "finalize_user_profile"
    ])
    conn.commit()
    
    # Run the worker
    _run_worker(username)
    print(f"[api] restart done username={username}")
    return jsonify({"ok": True, "queued": True, "forced": True})


@app.post("/users/<username>/refresh")
def refresh(username: str):
    """Refresh (deprecated, use /restart instead)."""
    print(f"[api] refresh called username={username} (deprecated; use /restart)")
    return restart(username)


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


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
