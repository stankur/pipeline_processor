import time
import os
from datetime import datetime
from psycopg import connect, Connection, Error
from psycopg.rows import dict_row
from models import UserSubject, RepoSubject, Recommendation, ItemType


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


def migrate_db() -> None:
    """Run database migrations for existing tables (idempotent)."""
    conn = get_conn()
    
    # Migration 1: Rename user_fingerprint -> judgment_fingerprint
    result = conn.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='recommendations' AND column_name='user_fingerprint'
    """).fetchone()
    
    if result:
        print("[db] Running migration: rename user_fingerprint -> judgment_fingerprint")
        conn.execute("ALTER TABLE recommendations RENAME COLUMN user_fingerprint TO judgment_fingerprint")
        conn.commit()
    
    # Migration 2: Drop old prompt_hash column if exists
    result = conn.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='recommendations' AND column_name='prompt_hash'
    """).fetchone()
    
    if result:
        print("[db] Running migration: drop prompt_hash column")
        conn.execute("ALTER TABLE recommendations DROP COLUMN prompt_hash")
        conn.commit()
    
    print("[db] Migrations complete")


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
    # Recommendations table: merged judgment + exposure + fingerprints
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS recommendations (
          user_id               TEXT NOT NULL,
          item_type             TEXT NOT NULL,   -- 'repo' (future: 'article', etc.)
          item_id               TEXT NOT NULL,   -- native id, e.g. 'owner/repo'
          include               BOOLEAN,         -- cached boolean judgment from LLM
          judged_at             REAL,            -- epoch seconds
          times_shown           INTEGER DEFAULT 0,
          last_shown_at         REAL,            -- epoch seconds, nullable
          judgment_fingerprint  TEXT,            -- Hash of user prompt + model config
          PRIMARY KEY (user_id, item_type, item_id)
        );
        """
    )
    conn.commit()
    
    # Run migrations after table creation
    migrate_db()
    
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
    Sorted by GitHub's pushed_at timestamp (descending)
    """
    rows = conn.execute(
        """
        SELECT s.subject_id, s.data_json, s.updated_at, l.include_reason, l.user_commit_days
        FROM user_repo_links l
        JOIN subjects s ON s.subject_type='repo' AND s.subject_id = l.repo_id
        WHERE l.username = %s
        """,
        (username,),
    ).fetchall()
    
    # Sort by GitHub's pushed_at from the JSON (strict - will raise if data is malformed)
    def get_github_timestamp(row: dict) -> float:
        data_json = row.get("data_json")
        if not data_json:
            raise ValueError(f"Missing data_json for repo {row.get('subject_id')}")
        
        repo = RepoSubject.model_validate_json(data_json)
        ts_str = repo.pushed_at or repo.updated_at
        if not ts_str:
            raise ValueError(f"Missing pushed_at/updated_at for repo {repo.id}")
        
        dt = datetime.fromisoformat(ts_str)
        return dt.timestamp()
    
    return sorted(rows, key=get_github_timestamp, reverse=True)


def select_best_repo_candidate(
    candidates: list[tuple[RepoSubject, dict]], author_username: str
) -> tuple[RepoSubject, dict]:
    """Select the best repo from candidates with same name.
    
    Selection criteria (in order of priority):
    1. Highest user_commit_days (None treated as -1)
    2. Prefer external original (owner != author_username)
    3. Newest subjects.updated_at timestamp
    
    Args:
        candidates: List of (RepoSubject, row_dict) tuples
        author_username: Username of the author to compare against
        
    Returns:
        tuple: (best_repo, best_row) - the selected candidate
    """
    def _selection_key(pair: tuple[RepoSubject, dict]) -> tuple:
        repo, row = pair
        subj_id = row.get("subject_id") or ""
        owner = subj_id.split("/", 1)[0] if "/" in subj_id else ""
        # Primary: user_commit_days (None -> -1)
        days = row.get("user_commit_days")
        days_val = days if isinstance(days, int) else -1
        # Secondary: prefer external (owner != author) -> rank 1
        external_flag = 1 if owner != author_username else 0
        # Tertiary: newer updated_at
        upd = row.get("updated_at") or 0
        return (days_val, external_flag, upd)
    
    return max(candidates, key=_selection_key)


def iter_users_with_highlights(conn: Connection):
    """Yield (username, UserSubject) for users with highlighted_repos.
    
    Queries all user subjects and yields only those with at least one highlighted repo.
    Skips invalid JSON or users without highlights.
    
    Yields:
        tuple: (username, UserSubject) pairs
    """
    rows = conn.execute(
        "SELECT subject_id, data_json FROM subjects WHERE subject_type='user'"
    ).fetchall()
    
    for row in rows:
        if not row.get("data_json"):
            continue
        try:
            user = UserSubject.model_validate_json(row["data_json"])
        except Exception:
            continue
        if not user.highlighted_repos:
            continue
        
        username = str(row.get("subject_id") or "").strip()
        if not username:
            continue
        
        yield (username, user)




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


def delete_user_recommendations(conn: Connection, username: str) -> None:
    """Delete all cached recommendations for a user.
    
    Useful for clearing feed cache when prompt logic changes or for testing fresh state.
    Does NOT affect work_items or repo subjects - only clears recommendation judgments.
    """
    print(f"[db] delete_user_recommendations user={username}")
    result = conn.execute(
        "DELETE FROM recommendations WHERE user_id=%s",
        (username,),
    )
    count = result.rowcount
    print(f"[db] delete_user_recommendations ok user={username} deleted={count}")


def delete_user_completely(conn: Connection, username: str) -> None:
    """Delete a user and all their associated resources.
    
    Removes:
    - User subject
    - All work items (user-scoped and repo-scoped)
    - User-repo links
    - Owned repo subjects (username/*)
    """
    print(f"[db] delete_user_completely user={username}")
    
    # Delete work items for user-scoped tasks
    conn.execute(
        "DELETE FROM work_items WHERE subject_type='user' AND subject_id=%s",
        (username,),
    )
    
    # Delete work items for repos owned by this user
    conn.execute(
        """
        DELETE FROM work_items 
        WHERE subject_type='repo' AND subject_id IN (
            SELECT repo_id FROM user_repo_links WHERE username=%s
        )
        """,
        (username,),
    )
    
    # Delete user-repo links
    conn.execute(
        "DELETE FROM user_repo_links WHERE username=%s",
        (username,),
    )
    
    # Delete owned repo subjects
    conn.execute(
        "DELETE FROM subjects WHERE subject_type='repo' AND subject_id LIKE %s",
        (f"{username}/%",),
    )
    
    # Delete user subject
    conn.execute(
        "DELETE FROM subjects WHERE subject_type='user' AND subject_id=%s",
        (username,),
    )
    
    print(f"[db] delete_user_completely ok user={username}")


# -------------------- Pydantic-based helpers --------------------


def upsert_user_subject(conn: Connection, username: str, user_model: UserSubject) -> None:
    """Insert or update a user subject using a validated Pydantic model.
    
    Args:
        conn: Database connection
        username: GitHub username (subject_id)
        user_model: UserSubject Pydantic model instance
    """
    if not isinstance(user_model, UserSubject):
        raise TypeError(f"Expected UserSubject, got {type(user_model)}")
    json_str = user_model.model_dump_json(exclude_none=False)
    upsert_subject(conn, "user", username, json_str)


def upsert_repo_subject(conn: Connection, repo_id: str, repo_model: RepoSubject) -> None:
    """Insert or update a repo subject using a validated Pydantic model.
    
    Args:
        conn: Database connection
        repo_id: Repository full name "owner/repo" (subject_id)
        repo_model: RepoSubject Pydantic model instance
    """
    if not isinstance(repo_model, RepoSubject):
        raise TypeError(f"Expected RepoSubject, got {type(repo_model)}")
    json_str = repo_model.model_dump_json(exclude_none=False)
    upsert_subject(conn, "repo", repo_id, json_str)


def get_user_subject(conn: Connection, username: str) -> UserSubject | None:
    """Fetch and validate a user subject as a Pydantic model.
    
    Args:
        conn: Database connection
        username: GitHub username (subject_id)
        
    Returns:
        UserSubject instance or None if not found or invalid
    """
    row = get_subject(conn, "user", username)
    if not row or not row.get("data_json"):
        return None
    try:
        return UserSubject.model_validate_json(row["data_json"])
    except Exception as e:
        print(f"[db] get_user_subject validation error user={username} err={e}")
        return None


def get_repo_subject(conn: Connection, repo_id: str) -> RepoSubject | None:
    """Fetch and validate a repo subject as a Pydantic model.
    
    Args:
        conn: Database connection
        repo_id: Repository full name "owner/repo" (subject_id)
        
    Returns:
        RepoSubject instance or None if not found or invalid
    """
    row = get_subject(conn, "repo", repo_id)
    if not row or not row.get("data_json"):
        return None
    try:
        return RepoSubject.model_validate_json(row["data_json"])
    except Exception as e:
        print(f"[db] get_repo_subject validation error repo={repo_id} err={e}")
        return None


# -------------------- Recommendations (For You feed) --------------------


def get_recommendation(conn: Connection, user_id: str, item_type: ItemType, item_id: str) -> Recommendation | None:
    """Fetch a single recommendation row or None."""
    row = conn.execute(
        "SELECT user_id, item_type, item_id, include, judged_at, times_shown, last_shown_at, judgment_fingerprint "
        "FROM recommendations WHERE user_id=%s AND item_type=%s AND item_id=%s",
        (user_id, item_type, item_id),
    ).fetchone()
    if not row:
        return None
    try:
        return Recommendation.model_validate(row)
    except Exception as e:
        print(f"[db] get_recommendation validation error user={user_id} type={item_type} id={item_id} err={e}")
        return None


def get_cached_recommendations(conn: Connection, user_id: str) -> list[Recommendation]:
    """Fetch all cached recommendations for a user where include=true.
    
    Returns only items that have been judged and marked for inclusion.
    Used for fast feed rendering without re-judging candidates.
    """
    rows = conn.execute(
        "SELECT user_id, item_type, item_id, include, judged_at, times_shown, last_shown_at, judgment_fingerprint "
        "FROM recommendations WHERE user_id=%s AND include=true",
        (user_id,)
    ).fetchall()
    
    recommendations = []
    for row in rows:
        try:
            recommendations.append(Recommendation.model_validate(row))
        except Exception as e:
            print(f"[db] get_cached_recommendations validation error user={user_id} item={row.get('item_id')} err={e}")
            continue
    
    return recommendations


def upsert_recommendation_judgment(
    conn: Connection,
    user_id: str,
    item_type: ItemType,
    item_id: str,
    include: bool,
    judgment_fingerprint: str,
    judged_at: float | None = None,
) -> None:
    """Insert or update a recommendation judgment (preserves exposure counters)."""
    judged_at = judged_at or time.time()
    try:
        print(f"[db] upsert_recommendation_judgment user={user_id} type={item_type} id={item_id} include={include}")
        conn.execute(
            """
            INSERT INTO recommendations(user_id, item_type, item_id, include, judged_at, times_shown, judgment_fingerprint)
            VALUES (%s, %s, %s, %s, %s, 0, %s)
            ON CONFLICT(user_id, item_type, item_id)
            DO UPDATE SET 
              include=excluded.include,
              judged_at=excluded.judged_at,
              judgment_fingerprint=excluded.judgment_fingerprint
            """,
            (user_id, item_type, item_id, include, judged_at, judgment_fingerprint),
        )
        print(f"[db] upsert_recommendation_judgment ok user={user_id} type={item_type} id={item_id}")
    except Error as e:
        print(f"[db] upsert_recommendation_judgment error user={user_id} type={item_type} id={item_id} err={e}")
        raise


def bump_recommendation_exposures(
    conn: Connection,
    user_id: str,
    items: list[tuple[ItemType, str]],  # (item_type, item_id)
    now_epoch: float | None = None,
) -> None:
    """Increment times_shown and update last_shown_at for shown items."""
    now_epoch = now_epoch or time.time()
    for item_type, item_id in items:
        try:
            conn.execute(
                """
                INSERT INTO recommendations(user_id, item_type, item_id, include, judged_at, times_shown, last_shown_at)
                VALUES (%s, %s, %s, NULL, NULL, 1, %s)
                ON CONFLICT(user_id, item_type, item_id)
                DO UPDATE SET 
                  times_shown = COALESCE(recommendations.times_shown, 0) + 1,
                  last_shown_at = excluded.last_shown_at
                """,
                (user_id, item_type, item_id, now_epoch),
            )
        except Error as e:
            print(f"[db] bump_recommendation_exposures error user={user_id} type={item_type} id={item_id} err={e}")
            # continue on error so partial bump succeeds


def get_user_languages(conn: Connection, username: str) -> list[str]:
    """Extract unique languages from user's repos.
    
    Returns list of unique language strings used in all the user's repos.
    """
    repo_rows = get_user_repos(conn, username)
    languages: set[str] = set()
    
    for rr in repo_rows:
        data_json = rr.get("data_json")
        if not data_json:
            continue
        try:
            repo = RepoSubject.model_validate_json(data_json)
            if repo.language:
                languages.add(repo.language)
        except Exception:
            continue
    
    return list(languages)


def get_trending_repos_by_languages(conn: Connection, languages: list[str]) -> list[dict]:
    """Query trending_repo subjects matching any language in list.
    
    Returns list of dicts with keys: subject_id, data_json
    """
    if not languages:
        return []
    
    placeholders = ','.join(['%s'] * len(languages))
    query = f"""
        SELECT subject_id, data_json
        FROM subjects
        WHERE subject_type = 'trending_repo'
        AND data_json::json->>'language' IN ({placeholders})
        ORDER BY updated_at DESC
    """
    
    rows = conn.execute(query, tuple(languages)).fetchall()
    return [dict(row) for row in rows]


def get_newest_trending_repo_age_hours(conn: Connection) -> float | None:
    """Get age in hours of newest trending_repo subject.
    
    Returns None if no trending repos exist.
    """
    row = conn.execute(
        """
        SELECT MAX(updated_at) as newest
        FROM subjects
        WHERE subject_type = 'trending_repo'
        """
    ).fetchone()
    
    if not row or not row['newest']:
        return None
    
    newest_iso = row['newest']
    try:
        from datetime import datetime, timezone
        newest_dt = datetime.fromisoformat(newest_iso)
        now = datetime.now(timezone.utc)
        age_hours = (now - newest_dt).total_seconds() / 3600.0
        return age_hours
    except Exception:
        return None


def get_cached_recommendations_by_type(conn: Connection, user_id: str, item_type: ItemType) -> list[Recommendation]:
    """Get cached recommendations for specific item_type where include=true.
    
    Args:
        user_id: Username
        item_type: Type filter (e.g., 'repo', 'trending_repo')
    
    Returns list of Recommendation objects.
    """
    rows = conn.execute(
        """
        SELECT user_id, item_type, item_id, include, judged_at, times_shown, last_shown_at, judgment_fingerprint
        FROM recommendations
        WHERE user_id = %s AND item_type = %s AND include = TRUE
        ORDER BY judged_at DESC
        """,
        (user_id, item_type)
    ).fetchall()
    
    recommendations = []
    for row in rows:
        try:
            rec = Recommendation(
                user_id=row['user_id'],
                item_type=row['item_type'],
                item_id=row['item_id'],
                include=row['include'],
                judged_at=row['judged_at'],
                times_shown=row['times_shown'],
                last_shown_at=row['last_shown_at'],
                judgment_fingerprint=row['judgment_fingerprint'],
            )
            recommendations.append(rec)
        except Exception:
            continue
    
    return recommendations


def get_all_highlighted_repos_with_gallery(conn: Connection) -> list[dict]:
    """Get all highlighted repos with gallery images from all users.
    
    Returns list of dicts with keys: repo_id, repo, author_username, github_timestamp.
    If multiple users highlight the same repo, attributes it to the first user.
    """
    global_repo_map: dict[str, tuple[RepoSubject, dict, str]] = {}
    
    for author_username, user in iter_users_with_highlights(conn):
        repo_rows = get_user_repos(conn, author_username)
        
        name_to_candidates: dict[str, list[tuple[RepoSubject, dict]]] = {}
        for repo_row in repo_rows:
            data_json = repo_row.get("data_json")
            if not data_json:
                continue
            try:
                repo = RepoSubject.model_validate_json(data_json)
            except Exception:
                continue
            name_to_candidates.setdefault(repo.name, []).append((repo, repo_row))
        
        for repo_name in user.highlighted_repos:
            candidates = name_to_candidates.get(repo_name)
            if not candidates:
                continue
            
            repo, repo_row = select_best_repo_candidate(candidates, author_username)
            
            if not repo.gallery:
                continue
            
            if repo.id not in global_repo_map:
                global_repo_map[repo.id] = (repo, repo_row, author_username)
    
    results = []
    for repo_id, (repo, repo_row, author_username) in global_repo_map.items():
        timestamp_str = repo.pushed_at or repo.updated_at
        if not timestamp_str:
            continue
        
        try:
            github_timestamp = datetime.fromisoformat(timestamp_str).timestamp()
        except Exception:
            continue
        
        results.append({
            "repo_id": repo_id,
            "repo": repo,
            "author_username": author_username,
            "github_timestamp": github_timestamp,
        })
    
    return results
