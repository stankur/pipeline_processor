import time
import os
from psycopg import connect, Connection, Error
from psycopg.rows import dict_row


def get_conn() -> Connection:
    """Return a Postgres connection with dict-row access.

    Uses DATABASE_URL, defaulting to local Supabase if unset.
    """
    default_host = "host.docker.internal" if os.path.exists("/.dockerenv") else "localhost"
    dsn = os.environ.get(
        "DATABASE_URL",
        f"postgresql://postgres:postgres@{default_host}:54322/postgres",
    )
    conn = connect(dsn, row_factory=dict_row)
    return conn


def init_db() -> None:
    """Create tables and indexes if they don't exist (idempotent)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS subjects (
          subject_type TEXT NOT NULL,
          subject_id   TEXT NOT NULL,
          data_json    TEXT,
          updated_at   REAL,
          PRIMARY KEY (subject_type, subject_id)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS work_items (
          id           TEXT PRIMARY KEY,
          kind         TEXT NOT NULL,
          subject_type TEXT NOT NULL,
          subject_id   TEXT NOT NULL,
          status       TEXT NOT NULL,
          output_json  TEXT,
          processed_at REAL
        );
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_work_item
        ON work_items(kind, subject_type, subject_id);
        """
    )
    # Link table: associates a username with repos to include (owned, active fork, or contributed)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_repo_links (
          username         TEXT NOT NULL,
          repo_id          TEXT NOT NULL,   -- subjects.subject_id (owner/repo)
          include_reason   TEXT,            -- 'owned' | 'fork_active' | 'contributed'
          user_commit_days INTEGER,         -- cached unique commit days by this user (nullable)
          updated_at       REAL,
          PRIMARY KEY (username, repo_id)
        );
        """
    )
    conn.commit()
    print(f"[db] init_db done")


def upsert_subject(conn: Connection, subject_type: str, subject_id: str, data_json: str) -> None:
    """Insert or update a subject snapshot JSON and bump its updated_at.

    subject_type: 'user' or 'repo'
    subject_id:   'alice' or 'alice/repo'
    data_json:    JSON string to store as the latest snapshot
    """
    now = time.time()
    try:
        print(f"[db] upsert_subject start {subject_type}:{subject_id} bytes={len(data_json) if data_json else 0}")
        conn.execute(
            """
            INSERT INTO subjects(subject_type, subject_id, data_json, updated_at)
            VALUES(%s, %s, %s, %s)
            ON CONFLICT(subject_type, subject_id)
            DO UPDATE SET data_json=excluded.data_json, updated_at=excluded.updated_at
            """,
            (subject_type, subject_id, data_json, now),
        )
        print(f"[db] upsert_subject ok {subject_type}:{subject_id}")
    except Error as e:
        print(f"[db] upsert_subject error {subject_type}:{subject_id} err={e}")
        raise


def get_subject(conn: Connection, subject_type: str, subject_id: str) -> dict | None:
    """Fetch a subject row (or None) for the given type/id."""
    return conn.execute(
        "SELECT subject_type, subject_id, data_json, updated_at FROM subjects WHERE subject_type=%s AND subject_id=%s",
        (subject_type, subject_id),
    ).fetchone()


def get_user_repos(conn: Connection, username: str) -> list[dict]:
    """List all repo subject rows linked to username via user_repo_links.

    Returns rows with columns: subject_id, data_json, updated_at, include_reason, user_commit_days
    """
    return conn.execute(
        """
        SELECT s.subject_id, s.data_json, s.updated_at, l.include_reason, l.user_commit_days
        FROM user_repo_links l
        JOIN subjects s ON s.subject_type='repo' AND s.subject_id = l.repo_id
        WHERE l.username = %s
        ORDER BY s.updated_at DESC
        """,
        (username,),
    ).fetchall()




def set_work_status(
    conn: Connection,
    kind: str,
    subject_type: str,
    subject_id: str,
    status: str,
    output_json: str | None = None,
) -> None:
    """Insert or update a work_item status and optionally its output_json/processed_at.

    If status is 'succeeded', processed_at is set to now.
    """
    import uuid
    processed_at = time.time() if status == "succeeded" else None
    try:
        print(
            f"[db] set_work_status kind={kind} subject={subject_type}:{subject_id} status={status} processed_at={processed_at}"
        )
        conn.execute(
            """
            INSERT INTO work_items(id, kind, subject_type, subject_id, status, output_json, processed_at)
            VALUES(%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(kind, subject_type, subject_id)
            DO UPDATE SET 
                status=excluded.status,
                output_json=COALESCE(excluded.output_json, work_items.output_json),
                processed_at=COALESCE(excluded.processed_at, work_items.processed_at)
            """,
            (uuid.uuid4().hex, kind, subject_type, subject_id, status, output_json, processed_at),
        )
        print(f"[db] set_work_status ok kind={kind} subject={subject_type}:{subject_id} status={status}")
    except Error as e:
        print(f"[db] set_work_status error kind={kind} subject={subject_type}:{subject_id} err={e}")
        raise


def list_user_work_items(conn: Connection, username: str) -> list[dict]:
    """List all work_items for a user-scoped subject (two tasks in this phase)."""
    return conn.execute(
        "SELECT kind, status, output_json, processed_at FROM work_items WHERE subject_type='user' AND subject_id=%s ORDER BY kind",
        (username,),
    ).fetchall()




def get_work_item(
    conn: Connection, kind: str, subject_type: str, subject_id: str
) -> dict | None:
    """Fetch a single work_item row if it exists, else None."""
    return conn.execute(
        "SELECT id, kind, subject_type, subject_id, status, output_json, processed_at FROM work_items WHERE kind=%s AND subject_type=%s AND subject_id=%s",
        (kind, subject_type, subject_id),
    ).fetchone()






def reset_user_work_items_to_pending(conn: Connection, username: str, kinds: list[str]) -> None:
    """Reset selected user work_items to pending for a force rerun."""
    print(f"[db] reset_work_items user={username} kinds={kinds}")
    qmarks = ",".join(["%s"] * len(kinds))
    params = [*kinds, username]
    conn.execute(
        f"UPDATE work_items SET status='pending', output_json=NULL, processed_at=NULL WHERE kind IN ({qmarks}) AND subject_type='user' AND subject_id=%s",
        params,
    )
    print(f"[db] reset_work_items ok user={username}")


def reset_user_repo_work_items_to_pending(conn: Connection, username: str, kinds: list[str]) -> None:
    """Reset selected repo-scoped work_items to pending for all repos linked to username.

    This targets work_items where subject_type='repo' and subject_id is in the user's current repo links.
    """
    print(f"[db] reset_repo_work_items user={username} kinds={kinds}")
    if not kinds:
        print(f"[db] reset_repo_work_items skipped (empty kinds) user={username}")
        return
    qmarks = ",".join(["%s"] * len(kinds))
    params = [*kinds, username]
    conn.execute(
        f"""
        UPDATE work_items
        SET status='pending', output_json=NULL, processed_at=NULL
        WHERE kind IN ({qmarks})
          AND subject_type='repo'
          AND subject_id IN (
                SELECT repo_id FROM user_repo_links WHERE username=%s
          )
        """,
        params,
    )
    print(f"[db] reset_repo_work_items ok user={username}")


def delete_user_repo_subjects(conn: Connection, username: str) -> None:
    """Delete owned repo subjects for a user and clear all their repo links.

    - Owned repos are identified by subject_id LIKE 'username/%' and will be removed.
    - Contributed repos (not owned) are kept as shared subjects, but links are removed.
    """
    print(f"[db] delete_user_repo_subjects user={username}")
    # Remove links first
    conn.execute(
        "DELETE FROM user_repo_links WHERE username=%s",
        (username,),
    )
    # Remove owned repo subjects
    conn.execute(
        "DELETE FROM subjects WHERE subject_type='repo' AND subject_id LIKE %s",
        (f"{username}/%",),
    )
    print(f"[db] delete_user_repo_subjects ok user={username}")


def upsert_user_repo_link(
    conn: Connection,
    username: str,
    repo_id: str,
    include_reason: str | None,
    user_commit_days: int | None,
) -> None:
    """Insert/update a link between a user and a repo with metadata."""
    now = time.time()
    try:
        print(
            f"[db] upsert_user_repo_link user={username} repo={repo_id} reason={include_reason} days={user_commit_days}"
        )
        conn.execute(
            """
            INSERT INTO user_repo_links(username, repo_id, include_reason, user_commit_days, updated_at)
            VALUES(%s, %s, %s, %s, %s)
            ON CONFLICT(username, repo_id)
            DO UPDATE SET include_reason=excluded.include_reason,
                          user_commit_days=excluded.user_commit_days,
                          updated_at=excluded.updated_at
            """,
            (username, repo_id, include_reason, user_commit_days, now),
        )
        print(f"[db] upsert_user_repo_link ok user={username} repo={repo_id}")
    except Error as e:
        print(f"[db] upsert_user_repo_link error user={username} repo={repo_id} err={e}")
        raise


def delete_user_repo_links(conn: Connection, username: str) -> None:
    """Delete all user-to-repo links for a username."""
    print(f"[db] delete_user_repo_links user={username}")
    conn.execute(
        "DELETE FROM user_repo_links WHERE username=%s",
        (username,),
    )
    print(f"[db] delete_user_repo_links ok user={username}")
