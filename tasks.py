import json
from datetime import datetime, timezone, timedelta

import os
import time
import json
from typing import Any, Dict, List, Optional

import yaml
import tiktoken
from dotenv import load_dotenv

from db import (
    get_conn,
    upsert_subject,
    set_work_status,
    get_subject,
    get_user_repos,
    get_work_item,
    upsert_user_repo_link,
    upsert_user_subject,
    upsert_repo_subject,
    get_user_subject,
    get_repo_subject,
    update_repo_embedding,
    update_user_embedding,
    update_trending_repo_embedding,
    get_subject_with_embedding,
    get_user_context,
    update_context_embedding,
)
from embeddings import (
    format_repo_for_embedding,
    format_user_for_embedding,
    compute_embedding_hash,
    get_embedding,
    get_embeddings_batch,
)
from github_client import GitHubClient
from utils import parse_llm_json
from models import (
    UserSubject,
    RepoSubject,
    GalleryImage,
    FetchProfileOutput,
    FetchReposOutput,
    InferUserThemeOutput,
    EnhanceRepoMediaOutput,
    GenerateRepoBlurbOutput,
    ExtractRepoEmphasisOutput,
    ExtractRepoKeywordsOutput,
    ExtractRepoKindOutput,
)

from logging import getLogger

logger = getLogger(__name__)


def _already_succeeded(conn, kind: str, subject_type: str, subject_id: str) -> bool:
    row = get_work_item(conn, kind, subject_type, subject_id)
    return bool(row and row.status == "succeeded")


def _recent_enough(ts: str | None, years: int = 2) -> bool:
    if not ts:
        return True  # be permissive if missing
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt >= (datetime.now(timezone.utc) - timedelta(days=years * 365))
    except Exception:
        return True


# Activity thresholds and limits
ACTIVITY_DAYS_THRESHOLD = 4
RECENT_YEARS = 2
MAX_BRANCHES_TO_SCAN = 20


def _count_author_unique_commit_days(
    client: GitHubClient,
    owner: str,
    repo: str,
    author: str,
    max_branches: int = MAX_BRANCHES_TO_SCAN,
    short_circuit_at: int = ACTIVITY_DAYS_THRESHOLD,
) -> int:
    """Count unique commit days by author across up to N branches, short-circuiting when threshold is met."""
    try:
        branches = client.list_branches(owner, repo, per_page=100, limit=max_branches)
    except Exception:
        branches = []
    seen_days: set[str] = set()
    for br in branches:
        name = (br.get("name") or "").strip()
        if not name:
            continue
        try:
            commits = client.list_commits(
                owner, repo, author=author, sha=name, per_page=100, limit=100
            )
        except Exception:
            commits = []
        for c in commits:
            try:
                meta = c.get("commit", {})
                date_str = meta.get("author", {}).get("date") or meta.get(
                    "committer", {}
                ).get("date")
                if date_str:
                    seen_days.add(date_str.split("T")[0])
                if len(seen_days) >= short_circuit_at:
                    return len(seen_days)
            except Exception:
                continue
    return len(seen_days)


def fetch_profile(username: str) -> None:
    """Fetch GitHub profile JSON for username and store it under subjects('user', username).

    Also marks the corresponding work_item status to 'running'->'succeeded' with a tiny summary.
    """
    print(f"[task] fetch_profile start username={username}")
    conn = get_conn()
    if _already_succeeded(conn, "fetch_profile", "user", username):
        return
    set_work_status(conn, "fetch_profile", "user", username, "running")
    client = GitHubClient()
    profile = client.get_user(username)
    print(f"[task] fetch_profile got profile login={profile.get('login')}")

    # Read existing user to preserve is_ghost flag
    existing = get_user_subject(conn, username)
    is_ghost = existing.is_ghost if existing else False

    user = UserSubject(
        login=profile.get("login") or username,
        avatar_url=profile.get("avatar_url"),
        bio=profile.get("bio"),
        location=profile.get("location"),
        blog=profile.get("blog"),
        is_ghost=is_ghost,
    )
    upsert_user_subject(conn, username, user)
    print(f"[task] fetch_profile upserted username={username}")

    output = FetchProfileOutput(profile_found=True)
    set_work_status(
        conn, "fetch_profile", "user", username, "succeeded", output.model_dump_json()
    )
    conn.commit()
    print(f"[task] fetch_profile commit username={username}")
    print(f"[task] fetch_profile done username={username}")


def fetch_repos(username: str) -> None:
    """Fetch and filter repositories for username; write only kept repos as lean subjects('repo','user/repo')."""
    print(f"[task] fetch_repos start username={username}")
    conn = get_conn()
    if _already_succeeded(conn, "fetch_repos", "user", username):
        return
    set_work_status(conn, "fetch_repos", "user", username, "running")
    client = GitHubClient()

    # Fetch recent repos (cap at 30 to limit API calls)
    repos = client.list_repos(username, sort="pushed", per_page=30, limit=30)
    print(f"[task] fetch_repos fetched count={len(repos)}")

    kept_subjects: list[dict] = []

    for repo in repos:
        name = repo.get("name")
        owner_login = (repo.get("owner") or {}).get("login") or username
        full_name = repo.get("full_name") or f"{owner_login}/{name}"
        # language
        if not repo.get("language"):
            continue
        # recency (2 years)
        pushed = repo.get("pushed_at") or repo.get("updated_at")
        if not _recent_enough(pushed, years=RECENT_YEARS):
            continue
        is_fork = bool(repo.get("fork"))

        # Build validated repo subject
        repo_subject = RepoSubject(
            id=full_name,
            name=name or "",
            description=repo.get("description"),
            language=repo.get("language"),
            stargazers_count=repo.get("stargazers_count", 0),
            fork=is_fork,
            pushed_at=repo.get("pushed_at"),
            updated_at=repo.get("updated_at"),
            topics=repo.get("topics", []),
        )

        include_reason: str | None = None
        user_commit_days: int | None = None

        if not is_fork:
            include_reason = "owned"
        else:
            # Include active forks only if author has >= threshold unique commit days across branches
            days = _count_author_unique_commit_days(client, owner_login, name, username)
            user_commit_days = days
            if days >= ACTIVITY_DAYS_THRESHOLD:
                include_reason = "fork_active"

        if include_reason:
            kept_subjects.append(repo_subject.model_dump())
            upsert_repo_subject(conn, full_name, repo_subject)
            upsert_user_repo_link(
                conn, username, full_name, include_reason, user_commit_days
            )

    print(f"[task] fetch_repos kept owned+forks count={len(kept_subjects)}")

    # Discover contributed repos (non-owned) within recent window via GraphQL
    # GraphQL 'contributionsCollection' enforces <= 1 year spans, so aggregate over rolling windows
    try:
        now = datetime.now(timezone.utc)
        lookback_start = now - timedelta(days=RECENT_YEARS * 365)
        window_days = 365  # GitHub GraphQL maximum window
        contributed_map: dict[str, dict] = {}

        cursor_to = now
        while cursor_to > lookback_start:
            cursor_from = max(lookback_start, cursor_to - timedelta(days=window_days))
            try:
                window_results = client.list_contributed_repos(
                    username,
                    cursor_from.isoformat(),
                    cursor_to.isoformat(),
                    limit=200,
                )
                print(
                    f"[task] fetch_repos contributions window {cursor_from.date()}..{cursor_to.date()} => {len(window_results)}"
                )
            except Exception as e:
                print(
                    f"[task] fetch_repos contributions window {cursor_from.date()}..{cursor_to.date()} ERROR: {e}"
                )
                window_results = []

            for r in window_results:
                owner = r.get("owner")
                name = r.get("name")
                if owner and name:
                    contributed_map[f"{owner}/{name}"] = r

            # Move window backwards
            cursor_to = cursor_from

        contributed = list(contributed_map.values())
        # Cap total contributed repos considered downstream
        if len(contributed) > 200:
            contributed = contributed[:200]
        print(f"[task] fetch_repos contributions aggregated unique={len(contributed)}")
    except Exception as e:
        print(f"[task] fetch_repos contributions discovery ERROR: {e}")
        contributed = []

    seen_ids = {s.get("id") for s in kept_subjects}
    for r in contributed:
        owner = r.get("owner")
        name = r.get("name")
        if not owner or not name:
            continue
        full_name = f"{owner}/{name}"
        if full_name in seen_ids:
            continue
        # Fetch repo metadata
        try:
            meta = client.get_repo(owner, name)
        except Exception:
            continue
        language = meta.get("language")
        if not language:
            continue
        pushed = meta.get("pushed_at") or meta.get("updated_at")
        if not _recent_enough(pushed, years=RECENT_YEARS):
            continue
        # Count author's commit days across branches
        days = _count_author_unique_commit_days(client, owner, name, username)
        if days < ACTIVITY_DAYS_THRESHOLD:
            continue

        repo_subject = RepoSubject(
            id=full_name,
            name=name,
            description=meta.get("description"),
            language=language,
            stargazers_count=meta.get("stargazers_count", 0),
            fork=bool(meta.get("fork")),
            pushed_at=meta.get("pushed_at"),
            updated_at=meta.get("updated_at"),
            topics=meta.get("topics", []),
        )
        upsert_repo_subject(conn, full_name, repo_subject)
        upsert_user_repo_link(conn, username, full_name, "contributed", days)
        kept_subjects.append(repo_subject.model_dump())
        seen_ids.add(full_name)

    output = FetchReposOutput(fetched=len(kept_subjects))
    set_work_status(
        conn, "fetch_repos", "user", username, "succeeded", output.model_dump_json()
    )
    conn.commit()
    print(f"[task] fetch_repos commit username={username}")
    print(f"[task] fetch_repos done username={username} kept={len(kept_subjects)}")


# -------------------- Prompt loading and LLM helpers --------------------

# Load .env file to get API key
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=False)
_OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()


def _load_prompts() -> Dict[str, Any]:
    """Load prompts from prompt.yaml in the same directory."""
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "prompt.yaml"))
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


def count_tokens(text: str, model_name: str = "gpt-4") -> int:
    """Count tokens in text using tiktoken for GPT models."""
    try:
        encoding = tiktoken.encoding_for_model("gpt-4")
        return len(encoding.encode(text))
    except Exception:
        return int(len(text.split()) * 1.3)  # rough tokens per word estimate


def _openrouter_chat(prompt: str, model: str = "deepseek/deepseek-chat-v3.1") -> str:
    if not _OPENROUTER_API_KEY:
        # In absence of API key, return empty string so caller can handle fallback
        return ""
    import requests  # local import

    headers = {
        "Authorization": f"Bearer {_OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
    }
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json=data,
        timeout=60,
    )
    resp.raise_for_status()
    j = resp.json()
    return (j.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()


# -------------------- New tasks: theme + highlights + enrichment --------------------


def select_highlighted_repos(username: str) -> None:
    """Choose highlighted repository names from kept repos via LLM.

    Stores output_json on work_item: {"repos": [repo_name, ...]} (can be empty).
    Does not write to subjects.
    """
    print(f"[task] select_highlighted_repos start username={username}")
    conn = get_conn()
    if _already_succeeded(conn, "select_highlighted_repos", "user", username):
        return
    set_work_status(conn, "select_highlighted_repos", "user", username, "running")

    rows = get_user_repos(conn, username)
    repos: List[Dict[str, Any]] = []
    for r in rows:
        try:
            obj = json.loads(r["data_json"]) if r["data_json"] else None
            if obj:
                # Attach link metadata for downstream filtering
                obj["_include_reason"] = (
                    r["include_reason"] if "include_reason" in r.keys() else None
                )
                obj["_user_commit_days"] = (
                    r["user_commit_days"] if "user_commit_days" in r.keys() else None
                )
                obj["_repo_id"] = r["subject_id"]
                repos.append(obj)
        except Exception:
            continue

    # Log kept repos overview
    try:
        kept_names = [r.get("name", "") for r in repos]
        print(
            f"[task] select_highlighted_repos kept_repos count={len(kept_names)} names={kept_names}"
        )
    except Exception:
        pass

    # Filter to strict criteria (README + >= ACTIVITY_DAYS_THRESHOLD unique commit days by user) for highlight selection
    client = GitHubClient()
    strict: List[Dict[str, Any]] = []
    for r in repos:
        try:
            repo_id = r.get("_repo_id") or ""
            if "/" not in repo_id:
                continue
            owner, name = repo_id.split("/", 1)
            readme_content, _ = client.get_repo_readme(owner, name)
            if not readme_content:
                continue
            days = r.get("_user_commit_days")
            if not isinstance(days, int):
                # Compute if not cached (owned repos path)
                days = _count_author_unique_commit_days(client, owner, name, username)
            if days < ACTIVITY_DAYS_THRESHOLD:
                continue
            strict.append(r)
        except Exception:
            continue
    repos = strict  # only nominees are considered for highlights
    print(f"[task] select_highlighted_repos strict_repos count={len(repos)}")

    prompts = _load_prompts()
    prompt_tmpl = prompts.get("enriching_prompt") if prompts else None
    names: List[str] = []

    print(f"[task] select_highlighted_repos prompt_tmpl={prompt_tmpl}")
    print(f"[task] select_highlighted_repos repos={repos}")

    if prompt_tmpl and repos:
        # Format similar to repo_formatter output
        lines: List[str] = []
        for repo in repos[:30]:
            lines.append(repo.get("name", ""))
            if repo.get("description"):
                lines.append(repo["description"])
            lines.append(repo.get("language", ""))
            stars = repo.get("stargazers_count", 0)
            lines.append(str(stars or 0))
            up = repo.get("pushed_at") or repo.get("updated_at")
            if up:
                lines.append(up)
            lines.append("")
        profile_blob = "\n".join(lines).strip()
        full_prompt = f"{prompt_tmpl}\n\n{profile_blob}"

        # Log full prompt
        print(
            f"[task] select_highlighted_repos PROMPT BEGIN\n{full_prompt}\nPROMPT END"
        )

        if count_tokens(full_prompt) < 5000:
            print(f"[task] select_highlighted_repos token count OK, calling LLM")
            try:
                text = _openrouter_chat(full_prompt)
                print(
                    f"[task] select_highlighted_repos LLM returned: {bool(text)} (length={len(text) if text else 0})"
                )
                if text:
                    # Log raw response
                    print(
                        f"[task] select_highlighted_repos RESPONSE BEGIN\n{text}\nRESPONSE END"
                    )
                    data = parse_llm_json(text)
                    print(f"[task] select_highlighted_repos JSON parsed: {data}")
                    rr = data.get("repos") if isinstance(data, dict) else None
                    if isinstance(rr, list):
                        names = [str(x).strip() for x in rr if str(x).strip()]
                        print(
                            f"[task] select_highlighted_repos extracted repo names: {names}"
                        )
                    else:
                        print(
                            f"[task] select_highlighted_repos repos field is not a list: {rr}"
                        )
                else:
                    print(
                        f"[task] select_highlighted_repos LLM returned empty response"
                    )
            except Exception as e:
                print(f"[task] select_highlighted_repos ERROR: {e}")
                names = []
        else:
            print(
                f"[task] select_highlighted_repos token count too high: {count_tokens(full_prompt)} >= 5000"
            )

    # Persist highlights immediately onto the user subject for early availability
    try:
        urow = get_subject(conn, "user", username)
        ubase = {}
        if urow and urow["data_json"]:
            try:
                ubase = json.loads(urow["data_json"]) or {}
            except Exception:
                ubase = {}
        ubase["highlighted_repos"] = names
        upsert_subject(conn, "user", username, json.dumps(ubase))
    except Exception:
        # Non-fatal: proceed to mark work item even if user subject update fails
        pass

    out = {"repos": names}
    print(
        f"[task] select_highlighted_repos parsed repos count={len(names)} names={names}"
    )
    set_work_status(
        conn, "select_highlighted_repos", "user", username, "succeeded", json.dumps(out)
    )
    conn.commit()
    print(
        f"[task] select_highlighted_repos done username={username} count={len(names)}"
    )


def infer_user_theme(username: str) -> None:
    """Infer a user theme using only the highlighted repos.

    Stores output_json: {"theme": str}. Also writes theme + highlighted_repos into user subject.
    """
    print(f"[task] infer_user_theme start username={username}")
    conn = get_conn()
    if _already_succeeded(conn, "infer_user_theme", "user", username):
        return
    set_work_status(conn, "infer_user_theme", "user", username, "running")

    # Read highlighted repos from previous task
    row = conn.execute(
        "SELECT output_json FROM work_items WHERE kind='select_highlighted_repos' AND subject_type='user' AND subject_id=%s AND status='succeeded'",
        (username,),
    ).fetchone()
    highlights: List[str] = []
    if row and row["output_json"]:
        try:
            data = json.loads(row["output_json"]) or {}
            rr = data.get("repos")
            if isinstance(rr, list):
                highlights = [str(x).strip() for x in rr if str(x).strip()]
        except Exception:
            highlights = []

    print(
        f"[task] infer_user_theme highlights count={len(highlights)} names={highlights}"
    )

    # Load kept repos and filter to highlighted subset for prompt input
    kept_rows = get_user_repos(conn, username)
    kept_by_name: Dict[str, Dict[str, Any]] = {}
    for r in kept_rows:
        try:
            obj = json.loads(r["data_json"]) if r["data_json"] else None
            if obj and obj.get("name"):
                kept_by_name[obj["name"]] = obj
        except Exception:
            continue
    subset = [kept_by_name[n] for n in highlights if n in kept_by_name]

    theme = ""
    prompts = _load_prompts()
    prompt_tmpl = prompts.get("enriching_prompt") if prompts else None
    if prompt_tmpl and subset:
        lines: List[str] = []
        for repo in subset[:30]:
            lines.append(repo.get("name", ""))
            if repo.get("description"):
                lines.append(repo["description"])
            lines.append(repo.get("language", ""))
            stars = repo.get("stargazers_count", 0)
            lines.append(str(stars or 0))
            up = repo.get("pushed_at") or repo.get("updated_at")
            if up:
                lines.append(up)
            lines.append("")
        profile_blob = "\n".join(lines).strip()
        full_prompt = f"{prompt_tmpl}\n\n{profile_blob}"
        print(f"[task] infer_user_theme PROMPT BEGIN\n{full_prompt}\nPROMPT END")
        if count_tokens(full_prompt) < 5000:
            print(f"[task] infer_user_theme token count OK, calling LLM")
            try:
                text = _openrouter_chat(full_prompt)
                print(
                    f"[task] infer_user_theme LLM returned: {bool(text)} (length={len(text) if text else 0})"
                )
                if text:
                    print(
                        f"[task] infer_user_theme RESPONSE BEGIN\n{text}\nRESPONSE END"
                    )
                    data = json.loads(text)
                    print(f"[task] infer_user_theme JSON parsed: {data}")
                    if isinstance(data, dict) and isinstance(data.get("theme"), str):
                        theme = data["theme"].strip()
                        print(f"[task] infer_user_theme extracted theme: {theme}")
                    else:
                        print(f"[task] infer_user_theme invalid data format: {data}")
                else:
                    print(f"[task] infer_user_theme LLM returned empty response")
            except Exception as e:
                print(f"[task] infer_user_theme ERROR: {e}")
                theme = ""
        else:
            print(
                f"[task] infer_user_theme token count too high: {count_tokens(full_prompt)} >= 5000"
            )

    # Persist into user subject
    user = get_user_subject(conn, username)
    if not user:
        user = UserSubject(login=username)

    user.theme = theme
    user.highlighted_repos = highlights
    upsert_user_subject(conn, username, user)

    print(f"[task] infer_user_theme parsed theme_len={len(theme)} theme={theme}")
    output = InferUserThemeOutput(theme=theme)
    set_work_status(
        conn,
        "infer_user_theme",
        "user",
        username,
        "succeeded",
        output.model_dump_json(),
    )
    conn.commit()
    print(
        f"[task] infer_user_theme done username={username} theme_len={len(theme)} highlights={len(highlights)}"
    )


def _capture_website_screenshot(url: str) -> Optional[bytes]:
    """Capture screenshot of website using ScreenshotOne API.

    Args:
        url: Website URL to screenshot

    Returns:
        Screenshot image bytes, or None if failed
    """
    try:
        import requests

        access_key = os.environ.get("SCREENSHOTONE_ACCESS_KEY", "").strip()
        if not access_key:
            print(f"[debug] SCREENSHOTONE_ACCESS_KEY not set, skipping screenshot")
            return None

        # Build ScreenshotOne API request
        api_url = "https://api.screenshotone.com/take"
        params = {
            "access_key": access_key,
            "url": url,
            "viewport_width": 1280,
            "viewport_height": 720,
            "device_scale_factor": 1,
            "format": "png",
            "block_ads": True,
            "block_cookie_banners": True,
            "delay": 4,  # Wait 2 seconds for page to render
        }

        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()

        return response.content
    except Exception as e:
        print(f"[debug] Failed to screenshot {url}: {e}")
        return None


def _upload_to_cloudinary(image_bytes: bytes, public_id: str) -> Optional[str]:
    """Upload image to Cloudinary and return URL.

    Args:
        image_bytes: Image data
        public_id: Cloudinary public ID for the image

    Returns:
        Cloudinary secure URL, or None if failed
    """
    try:
        import cloudinary
        import cloudinary.uploader

        # Configure Cloudinary (idempotent)
        cloudinary.config(
            cloud_name=os.environ.get("CLOUDINARY_CLOUD_NAME"),
            api_key=os.environ.get("CLOUDINARY_API_KEY"),
            api_secret=os.environ.get("CLOUDINARY_API_SECRET"),
        )

        result = cloudinary.uploader.upload(
            image_bytes, public_id=public_id, folder="repo_screenshots"
        )
        return result["secure_url"]
    except Exception as e:
        print(f"[debug] Failed to upload to Cloudinary: {e}")
        return None


def _extract_repo_media_data(
    owner: str, name: str
) -> tuple[Optional[str], List[Dict[str, Any]]]:
    """Extract demo images from repo README by checking actual dimensions.

    Args:
        owner: Repository owner
        name: Repository name

    Returns:
        (link, gallery_list) where:
        - link: Optional homepage URL from repo metadata
        - gallery_list: List of dicts with keys: url, alt, width, height, original_url, taken_at, is_highlight
    """
    import mistune
    from bs4 import BeautifulSoup
    from PIL import Image
    import requests
    from io import BytesIO

    client = GitHubClient()

    # Fetch README content and repo metadata
    readme_content, _ = client.get_repo_readme(owner, name)
    repo_meta = None
    try:
        repo_meta = client.get_repo(owner, name)
    except Exception:
        repo_meta = None

    # Extract homepage link
    link = None
    if repo_meta:
        homepage = repo_meta.get("homepage")
        if isinstance(homepage, str) and homepage.strip():
            h = homepage.strip()
            if h.startswith("//"):
                link = f"https:{h}"
            elif h.startswith("http://") or h.startswith("https://"):
                link = h
            else:
                link = f"https://{h}"

    # Get default branch
    default_branch = "main"
    if repo_meta and isinstance(repo_meta.get("default_branch"), str):
        default_branch = repo_meta.get("default_branch") or "main"

    if not readme_content:
        return link, []

    # Parse markdown to HTML
    html = mistune.html(readme_content)

    # Use BeautifulSoup to extract all <img> tags
    soup = BeautifulSoup(html, "html.parser")
    images = soup.find_all("img")

    # Extract and normalize URLs
    image_urls = []
    for img in images:
        url = img.get("src", "").strip()
        alt = img.get("alt", "").strip()
        if not url:
            continue

        original_url = url

        # Normalize relative URLs
        if not url.startswith("http://") and not url.startswith("https://"):
            url = f"https://github.com/{owner}/{name}/raw/{default_branch}/{url.lstrip('./').lstrip('/')}"

        image_urls.append({"url": url, "alt": alt, "original_url": original_url})

    # Quick pre-filter before fetching
    filtered_urls = []
    for item in image_urls:
        url = item["url"]
        url_lower = url.lower()

        # Skip known badge domains
        if any(
            x in url_lower
            for x in ["shields.io", "badge", "travis-ci.org", "circleci.com"]
        ):
            continue

        # Skip videos
        if any(url_lower.endswith(x) for x in [".mp4", ".webm", ".mov", ".avi"]):
            continue

        filtered_urls.append(item)

    # Helper function to fetch dimensions for a single image
    def fetch_image_dimensions(item: Dict[str, str]) -> Dict[str, Any]:
        url = item["url"]

        # Try partial download first (faster, saves bandwidth)
        try:
            response = requests.get(
                url, timeout=5, stream=True, headers={"Range": "bytes=0-50000"}
            )
            if response.status_code in (200, 206):  # 206 = Partial Content
                image_data = BytesIO(response.content)
                img = Image.open(image_data)
                return {**item, "width": img.width, "height": img.height, "error": None}
        except Exception:
            pass

        # Fallback to full download
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            image_data = BytesIO(response.content)
            img = Image.open(image_data)
            return {**item, "width": img.width, "height": img.height, "error": None}
        except Exception as e:
            return {**item, "width": None, "height": None, "error": str(e)}

    # Fetch dimensions in parallel (max 10 concurrent requests)
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(fetch_image_dimensions, item) for item in filtered_urls
        ]
        for future in as_completed(futures):
            results.append(future.result())

    # Filter by size and aspect ratio
    gallery: List[Dict[str, Any]] = []
    for result in results:
        if result["error"]:
            print(
                f"[debug] Failed to get dimensions for {result['url']}: {result['error']}"
            )
            continue

        width = result["width"]
        height = result["height"]

        if not width or not height:
            continue

        aspect_ratio = width / height

        # Must be reasonably large and rectangular-ish
        if width >= 400 and 0.3 <= aspect_ratio <= 4.0:
            taken_at = int(datetime.now(timezone.utc).timestamp() * 1000)
            gallery.append(
                {
                    "url": result["url"],
                    "alt": result["alt"],
                    "width": width,
                    "height": height,
                    "original_url": result["original_url"],
                    "taken_at": taken_at,
                    "is_highlight": True,
                }
            )
        else:
            # Log filtered images for debugging
            reasons = []
            if width < 400:
                reasons.append(f"too small (width={width}px)")
            if aspect_ratio < 0.3:
                reasons.append(f"too tall (aspect={aspect_ratio:.2f})")
            if aspect_ratio > 4.0:
                reasons.append(f"too wide (aspect={aspect_ratio:.2f})")
            print(
                f"[debug] Filtered out {result['url']}: {', '.join(reasons)} ({width}x{height})"
            )

    # If no images found but has homepage link, capture screenshot
    if not gallery and link:
        print(
            f"[debug] No README images found, attempting to screenshot website: {link}"
        )
        screenshot_bytes = _capture_website_screenshot(link)

        if screenshot_bytes:
            # Upload to Cloudinary
            public_id = f"{owner}_{name}".replace("/", "_")
            cloudinary_url = _upload_to_cloudinary(screenshot_bytes, public_id)

            if cloudinary_url:
                taken_at = int(datetime.now(timezone.utc).timestamp() * 1000)
                gallery.append(
                    {
                        "url": cloudinary_url,
                        "alt": f"Screenshot of {link}",
                        "width": 1280,
                        "height": 720,
                        "original_url": link,
                        "taken_at": taken_at,
                        "is_highlight": True,
                    }
                )
                print(
                    f"[debug] Successfully captured and uploaded screenshot: {cloudinary_url}"
                )
            else:
                print(f"[debug] Failed to upload screenshot to Cloudinary")
        else:
            print(f"[debug] Failed to capture screenshot")

    return link, gallery


def enhance_repo_media(repo_id: str) -> None:
    """Enhance a single repo subject with link and gallery extracted from README.

    repo_id: "user/repo"
    """
    print(f"[task] enhance_repo_media start repo={repo_id}")
    conn = get_conn()
    if _already_succeeded(conn, "enhance_repo_media", "repo", repo_id):
        return
    set_work_status(conn, "enhance_repo_media", "repo", repo_id, "running")

    try:
        owner, name = repo_id.split("/", 1)
    except ValueError:
        set_work_status(conn, "enhance_repo_media", "repo", repo_id, "failed")
        conn.commit()
        return

    # Extract media data
    link, gallery = _extract_repo_media_data(owner, name)

    # Merge into repo subject
    repo = get_repo_subject(conn, repo_id)
    if not repo:
        # Create minimal repo subject if missing (shouldn't happen normally)
        repo = RepoSubject(id=repo_id, name=repo_id.split("/")[-1])

    repo.link = link

    # Merge with existing gallery, dedupe by URL
    existing_urls: set[str] = {img.url for img in repo.gallery}

    num_added = 0
    for item in gallery:
        url = item.get("url", "").strip()
        if not url or url in existing_urls:
            continue
        gallery_img = GalleryImage(**item)
        repo.gallery.append(gallery_img)
        existing_urls.add(url)
        num_added += 1

    upsert_repo_subject(conn, repo_id, repo)

    output = EnhanceRepoMediaOutput(
        link=link, gallery_count=len(repo.gallery), added=num_added
    )
    set_work_status(
        conn,
        "enhance_repo_media",
        "repo",
        repo_id,
        "succeeded",
        output.model_dump_json(),
    )
    conn.commit()
    print(
        f"[task] enhance_repo_media done repo={repo_id} link={bool(link)} gallery={len(gallery)}"
    )


def generate_repo_blurb(repo_id: str) -> None:
    """Generate a concise LLM blurb for a repo using sophisticated codebase analysis.

    Uses the same logic as proj/scripts for actual codebase exploration.
    Stores under repo subject field 'generated_description'.
    """
    print(f"[task] generate_repo_blurb start repo={repo_id}")
    conn = get_conn()
    if _already_succeeded(conn, "generate_repo_blurb", "repo", repo_id):
        return
    set_work_status(conn, "generate_repo_blurb", "repo", repo_id, "running")

    try:
        owner, name = repo_id.split("/", 1)
    except ValueError:
        set_work_status(conn, "generate_repo_blurb", "repo", repo_id, "failed")
        conn.commit()
        return

    from repo_analyzer import clone_and_analyze_repo, BLURB_PROMPT

    desc = ""
    try:
        print(f"[task] generate_repo_blurb cloning and analyzing {repo_id}")

        # Use sophisticated repo analysis (same as scripts)
        context, files_included = clone_and_analyze_repo(
            owner, name, count_tokens, verbose=True
        )
        full_prompt = f"{context}\n\n{BLURB_PROMPT}"

        print(
            f"[task] generate_repo_blurb analyzed {len(files_included)} files: {files_included}"
        )
        print(f"[task] generate_repo_blurb token count: {count_tokens(full_prompt)}")

        if count_tokens(full_prompt) < 5500:
            print(f"[task] generate_repo_blurb calling LLM")
            desc = _openrouter_chat(full_prompt) or ""
            print(f"[task] generate_repo_blurb LLM response length: {len(desc)}")
        else:
            print(f"[task] generate_repo_blurb prompt too long, skipping LLM call")

    except Exception as e:
        print(f"[task] generate_repo_blurb ERROR during analysis: {e}")
        desc = ""

    # Merge into repo subject
    repo = get_repo_subject(conn, repo_id)
    if not repo:
        # Create minimal repo subject if missing
        repo = RepoSubject(id=repo_id, name=repo_id.split("/")[-1])

    if desc:
        repo.generated_description = desc
        upsert_repo_subject(conn, repo_id, repo)

    output = GenerateRepoBlurbOutput(description_len=len(desc))
    set_work_status(
        conn,
        "generate_repo_blurb",
        "repo",
        repo_id,
        "succeeded",
        output.model_dump_json(),
    )
    conn.commit()
    print(f"[task] generate_repo_blurb done repo={repo_id} has_desc={bool(desc)}")


def _embed_repos_batch_generic(
    repo_ids: list[str],
    subject_type: str,
    work_kind: str,
    update_embedding_func,
    log_context: str = "",
) -> None:
    """Generic batch embedding for any repo subject type.

    Args:
        repo_ids: List of repo IDs to embed
        subject_type: 'repo' or 'trending_repo'
        work_kind: Work item kind for tracking (e.g., 'embed_repo')
        update_embedding_func: Function to call to store embedding (takes conn, repo_id, embedding)
        log_context: Optional context string for logging (e.g., username)
    """
    log_prefix = f"{log_context} " if log_context else ""
    print(
        f"[task] _embed_repos_batch_generic start {log_prefix}type={subject_type} repo_count={len(repo_ids)}"
    )

    if not repo_ids:
        print(
            f"[task] _embed_repos_batch_generic: no repos to embed {log_prefix}type={subject_type}"
        )
        return

    conn = get_conn()

    # Collect repos that need embedding
    repos_to_embed = []
    for repo_id in repo_ids:
        repo, _ = get_subject_with_embedding(conn, subject_type, repo_id)

        if not repo:
            print(f"[task] {work_kind}: repo not found {repo_id}")
            continue

        # Format content and compute hash
        embedding_content = format_repo_for_embedding(repo)
        content_hash = compute_embedding_hash(embedding_content)

        # Check if already embedded with same content
        work_item = get_work_item(conn, work_kind, subject_type, repo_id)
        if work_item and work_item.status == "succeeded" and work_item.output_json:
            try:
                output_data = json.loads(work_item.output_json)
                stored_hash = output_data.get("content_hash")
                if stored_hash == content_hash:
                    print(f"[task] {work_kind}: cache hit {repo_id}")
                    continue
                else:
                    print(
                        f"[task] {work_kind}: content changed {repo_id}, re-embedding"
                    )
            except Exception:
                pass

        repos_to_embed.append((repo_id, embedding_content, content_hash))

    if not repos_to_embed:
        print(f"[task] {work_kind}: all repos already embedded {log_prefix}")
        return

    print(f"[task] {work_kind}: embedding {len(repos_to_embed)} repos {log_prefix}")

    # Process in batches of 20
    BATCH_SIZE = 20
    for i in range(0, len(repos_to_embed), BATCH_SIZE):
        batch = repos_to_embed[i : i + BATCH_SIZE]
        batch_ids = [item[0] for item in batch]

        print(
            f"[task] {work_kind}: batch {i//BATCH_SIZE + 1} ({len(batch)} repos): {batch_ids}"
        )

        # Mark all as running
        for repo_id, _, _ in batch:
            set_work_status(conn, work_kind, subject_type, repo_id, "running")
        conn.commit()

        # Call batch embedding API
        texts = [item[1] for item in batch]  # embedding_content
        try:
            embeddings = get_embeddings_batch(texts)
            print(f"[task] {work_kind}: received {len(embeddings)} embeddings")
        except Exception as e:
            print(f"[task] {work_kind}: batch API call failed: {e}")
            # Mark all as failed
            for repo_id, _, _ in batch:
                set_work_status(
                    conn,
                    work_kind,
                    subject_type,
                    repo_id,
                    "failed",
                    json.dumps({"reason": f"batch_api_error: {e}"}),
                )
            conn.commit()
            continue

        # Store embeddings for each repo
        for (repo_id, content, content_hash), embedding in zip(batch, embeddings):
            try:
                # Store embedding in database
                update_embedding_func(conn, repo_id, embedding)

                # Mark success with content hash
                output_json = json.dumps(
                    {"content_hash": content_hash, "embedding_dim": len(embedding)}
                )
                set_work_status(
                    conn, work_kind, subject_type, repo_id, "succeeded", output_json
                )
                print(f"[task] {work_kind}: stored embedding for {repo_id}")
            except Exception as e:
                print(
                    f"[task] {work_kind}: failed to store embedding for {repo_id}: {e}"
                )
                set_work_status(
                    conn,
                    work_kind,
                    subject_type,
                    repo_id,
                    "failed",
                    json.dumps({"reason": f"storage_error: {e}"}),
                )

        conn.commit()

    print(f"[task] {work_kind}: completed {log_prefix}")


def embed_repos_batch(username: str, repo_ids: list[str]) -> None:
    """Batch embed user repos.

    Embeds repos after blurb generation using format: name + description + generated_description + language.
    Processes repos in batches of 20 to minimize API calls.
    Stores embeddings in subjects.embedding and tracks content hash for change detection.

    Automatically re-embeds if content changes (hash mismatch).
    """
    _embed_repos_batch_generic(
        repo_ids=repo_ids,
        subject_type="repo",
        work_kind="embed_repo",
        update_embedding_func=update_repo_embedding,
        log_context=f"username={username}",
    )


def embed_user_profile(username: str) -> None:
    """Generate and store embedding for user profile.

    Embeds user after highlighted repos are selected and blurbed.
    Format: bio + concatenated highlighted repo descriptions
    Stores embedding in subjects.embedding WHERE subject_type='user'

    Automatically re-embeds if content changes (hash mismatch).
    """
    print(f"[task] embed_user_profile start username={username}")
    conn = get_conn()

    # Load user subject
    user = get_user_subject(conn, username)
    if not user:
        set_work_status(
            conn,
            "embed_user_profile",
            "user",
            username,
            "failed",
            json.dumps({"reason": "no_subject"}),
        )
        conn.commit()
        print(f"[task] embed_user_profile FAILED username={username} reason=no_subject")
        return

    # Get highlighted repo names from select_highlighted_repos work item
    highlighted_names = []
    work_item = get_work_item(conn, "select_highlighted_repos", "user", username)
    if work_item and work_item.status == "succeeded" and work_item.output_json:
        try:
            data = json.loads(work_item.output_json)
            highlighted_names = data.get("repos", [])
        except Exception:
            pass

    if not highlighted_names:
        print(
            f"[task] embed_user_profile: no highlighted repos for username={username}, skipping embedding"
        )
        set_work_status(
            conn,
            "embed_user_profile",
            "user",
            username,
            "succeeded",
            json.dumps({"reason": "no_highlights_skip", "embedding_dim": 0}),
        )
        conn.commit()
        return

    # Load full repo subjects for highlighted repos
    highlighted_repos = []
    repo_rows = get_user_repos(conn, username)
    repo_by_name = {}
    for repo_row in repo_rows:
        try:
            repo_data = (
                json.loads(repo_row["data_json"]) if repo_row["data_json"] else None
            )
            if repo_data and repo_data.get("name"):
                repo_by_name[repo_data["name"]] = repo_row
        except Exception:
            continue

    for repo_name in highlighted_names:
        if repo_name in repo_by_name:
            repo_row = repo_by_name[repo_name]
            try:
                repo = RepoSubject.model_validate_json(repo_row["data_json"])
                highlighted_repos.append(repo)
            except Exception as e:
                print(
                    f"[task] embed_user_profile: failed to parse repo {repo_name}: {e}"
                )
                continue

    # Format content for embedding
    embedding_content = format_user_for_embedding(user, highlighted_repos)
    content_hash = compute_embedding_hash(embedding_content)

    # Check if already embedded with same content
    work_item = get_work_item(conn, "embed_user_profile", "user", username)
    if work_item and work_item.status == "succeeded" and work_item.output_json:
        try:
            output_data = json.loads(work_item.output_json)
            stored_hash = output_data.get("content_hash")
            if stored_hash == content_hash:
                print(
                    f"[task] embed_user_profile cache hit username={username} (content unchanged)"
                )
                return
            else:
                print(
                    f"[task] embed_user_profile content changed username={username}, re-embedding"
                )
        except Exception:
            pass

    # Mark as running
    set_work_status(conn, "embed_user_profile", "user", username, "running")

    # Generate embedding
    try:
        print(f"[task] embed_user_profile calling embedding API username={username}")
        embedding = get_embedding(embedding_content)
        print(
            f"[task] embed_user_profile received embedding dim={len(embedding)} username={username}"
        )
    except Exception as e:
        set_work_status(
            conn,
            "embed_user_profile",
            "user",
            username,
            "failed",
            json.dumps({"reason": str(e)}),
        )
        conn.commit()
        print(f"[task] embed_user_profile FAILED username={username} error={e}")
        return

    # Store embedding in subjects table
    try:
        update_user_embedding(conn, username, embedding)
        print(f"[task] embed_user_profile stored embedding username={username}")
    except Exception as e:
        set_work_status(
            conn,
            "embed_user_profile",
            "user",
            username,
            "failed",
            json.dumps({"reason": f"storage_error: {e}"}),
        )
        conn.commit()
        print(
            f"[task] embed_user_profile FAILED to store embedding username={username} error={e}"
        )
        return

    # Mark success with content hash
    output_json = json.dumps(
        {"content_hash": content_hash, "embedding_dim": len(embedding)}
    )
    set_work_status(
        conn, "embed_user_profile", "user", username, "succeeded", output_json
    )
    conn.commit()
    print(f"[task] embed_user_profile done username={username}")


def embed_user_context(context_id: str) -> None:
    """Generate and store embedding for a user context.
    
    Args:
        context_id: ID of the context to embed (format: 'username:uuid')
    """
    print(f"[task] embed_user_context start context_id={context_id}")
    conn = get_conn()
    
    # Load context
    context = get_user_context(conn, context_id)
    
    if not context:
        print(f"[task] embed_user_context FAILED: context not found id={context_id}")
        return
    
    content = context.content.strip()
    if not content:
        print(f"[task] embed_user_context FAILED: empty content id={context_id}")
        return
    
    # Generate embedding
    try:
        print(f"[task] embed_user_context calling embedding API context_id={context_id}")
        embedding = get_embedding(content)
        print(f"[task] embed_user_context received embedding dim={len(embedding)} context_id={context_id}")
    except Exception as e:
        print(f"[task] embed_user_context FAILED context_id={context_id} error={e}")
        return
    
    # Store embedding
    try:
        update_context_embedding(conn, context_id, embedding)
        conn.commit()
        print(f"[task] embed_user_context done context_id={context_id}")
    except Exception as e:
        print(f"[task] embed_user_context FAILED to store embedding context_id={context_id} error={e}")
        return


def embed_trending_repos_batch(repo_ids: list[str]) -> None:
    """Embed batch of trending repos.

    Similar to embed_repos_batch but for trending_repo subject_type.
    Uses same embedding logic (name + description + language).
    Processes in batches of 20 to minimize API calls.
    """
    _embed_repos_batch_generic(
        repo_ids=repo_ids,
        subject_type="trending_repo",
        work_kind="embed_trending_repo",
        update_embedding_func=update_trending_repo_embedding,
        log_context="",
    )


# -------------------- New task: extract emphasis from generated_description --------------------


def extract_repo_emphasis(repo_id: str) -> None:
    """Extract technology emphasis array from generated_description and store it on the repo subject.

    Fails fast if preconditions or parsing fail; succeeds only when emphasis is extracted and saved.
    """
    print(f"[task] extract_repo_emphasis start repo={repo_id}")
    conn = get_conn()
    if _already_succeeded(conn, "extract_repo_emphasis", "repo", repo_id):
        return
    set_work_status(conn, "extract_repo_emphasis", "repo", repo_id, "running")

    # Load repo subject
    repo = get_repo_subject(conn, repo_id)
    if not repo:
        set_work_status(
            conn,
            "extract_repo_emphasis",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": "no_subject"}),
        )
        conn.commit()
        print(f"[task] extract_repo_emphasis FAILED repo={repo_id} reason=no_subject")
        return

    desc = repo.generated_description
    if not desc or not desc.strip():
        set_work_status(
            conn,
            "extract_repo_emphasis",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": "no_generated_description"}),
        )
        conn.commit()
        print(
            f"[task] extract_repo_emphasis FAILED repo={repo_id} reason=no_generated_description"
        )
        return

    prompt = (
        "can you extract an array parseable with json.loads, of the frameworks that this project is dependent on, "
        "languages, technologies, or libraries used. Just the array and nothing else, make the capitalization exactly as written:\n\n"
        f"{desc}"
    )

    # Call LLM and parse strictly
    try:
        text = _openrouter_chat(prompt)
        print(
            f"[task] extract_repo_keywords LLM returned: {bool(text)} (length={len(text) if text else 0})"
        )
        if text:
            print(
                f"[task] extract_repo_keywords LLM RESPONSE BEGIN\n{text}\nLLM RESPONSE END"
            )
        if not text:
            raise ValueError("empty_llm_response")
        data = parse_llm_json(text)
        if not isinstance(data, list):
            raise ValueError("not_array")
        if not all(isinstance(x, str) and x.strip() for x in data):
            raise ValueError("invalid_array_elements")
        emphasis_list = [x.strip() for x in data]
    except Exception as e:
        set_work_status(
            conn,
            "extract_repo_emphasis",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": str(e)}),
        )
        conn.commit()
        print(f"[task] extract_repo_emphasis FAILED repo={repo_id} reason={e}")
        return

    # Persist emphasis and mark success
    repo.emphasis = emphasis_list
    upsert_repo_subject(conn, repo_id, repo)

    output = ExtractRepoEmphasisOutput(emphasis=emphasis_list, count=len(emphasis_list))
    set_work_status(
        conn,
        "extract_repo_emphasis",
        "repo",
        repo_id,
        "succeeded",
        output.model_dump_json(),
    )
    conn.commit()
    print(
        f"[task] extract_repo_emphasis done repo={repo_id} count={len(emphasis_list)}"
    )


# -------------------- New task: extract keywords (skills) from generated_description --------------------


def extract_repo_keywords(repo_id: str) -> None:
    """Extract 1-4 conceptual skill keywords from generated_description and store on repo subject.

    Fails fast if preconditions or parsing fail; succeeds only when keywords are extracted and saved.
    """
    print(f"[task] extract_repo_keywords start repo={repo_id}")
    conn = get_conn()
    if _already_succeeded(conn, "extract_repo_keywords", "repo", repo_id):
        return
    set_work_status(conn, "extract_repo_keywords", "repo", repo_id, "running")

    # Load repo subject
    repo = get_repo_subject(conn, repo_id)
    if not repo:
        set_work_status(
            conn,
            "extract_repo_keywords",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": "no_subject"}),
        )
        conn.commit()
        print(f"[task] extract_repo_keywords FAILED repo={repo_id} reason=no_subject")
        return

    desc = repo.generated_description
    if not desc or not desc.strip():
        set_work_status(
            conn,
            "extract_repo_keywords",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": "no_generated_description"}),
        )
        conn.commit()
        print(
            f"[task] extract_repo_keywords FAILED repo={repo_id} reason=no_generated_description"
        )
        return

    prompt = (
        "can you extract the main keywords that represent the technical techniques/knowledge/concepts needed for this project, "
        "and output an array of 1-4 keywords, that will be used to display the skills of the person (don't list tools/frameworks/programming language):\n\n"
        f"{desc}"
    )

    # Call LLM and parse strictly
    try:
        print(f"[task] extract_repo_keywords PROMPT BEGIN\n{prompt}\nPROMPT END")
        text = _openrouter_chat(prompt)
        print(
            f"[task] extract_repo_keywords LLM returned: {bool(text)} (length={len(text) if text else 0})"
        )
        if text:
            print(
                f"[task] extract_repo_keywords LLM RESPONSE BEGIN\n{text}\nLLM RESPONSE END"
            )
        if not text:
            raise ValueError("empty_llm_response")
        data = parse_llm_json(text)
        if not isinstance(data, list):
            raise ValueError("not_array")
        if not (1 <= len(data) <= 4):
            raise ValueError("array_length_out_of_range")
        if not all(isinstance(x, str) and x.strip() for x in data):
            raise ValueError("invalid_array_elements")
        keywords_list = [x.strip() for x in data]
    except Exception as e:
        try:
            # Best-effort raw output logging to aid debugging when JSON parsing fails
            if "text" in locals() and text:
                print(
                    f"[task] extract_repo_keywords RAW RESPONSE BEGIN\n{text}\nRAW RESPONSE END"
                )
        except Exception:
            pass
        set_work_status(
            conn,
            "extract_repo_keywords",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": str(e)}),
        )
        conn.commit()
        print(f"[task] extract_repo_keywords FAILED repo={repo_id} reason={e}")
        return

    # Persist keywords and mark success
    repo.keywords = keywords_list
    upsert_repo_subject(conn, repo_id, repo)

    output = ExtractRepoKeywordsOutput(keywords=keywords_list, count=len(keywords_list))
    set_work_status(
        conn,
        "extract_repo_keywords",
        "repo",
        repo_id,
        "succeeded",
        output.model_dump_json(),
    )
    conn.commit()
    print(
        f"[task] extract_repo_keywords done repo={repo_id} count={len(keywords_list)}"
    )


def extract_repo_kind(repo_id: str) -> None:
    """Create a very short noun phrase kind from generated_description and store on repo subject.

    Minimal: no enforcement, no JSON parsing, just save whatever the LLM returns (trimmed).
    """
    print(f"[task] extract_repo_kind start repo={repo_id}")
    conn = get_conn()
    if _already_succeeded(conn, "extract_repo_kind", "repo", repo_id):
        return
    set_work_status(conn, "extract_repo_kind", "repo", repo_id, "running")

    # Load repo subject (best-effort, minimal)
    repo = get_repo_subject(conn, repo_id)
    if not repo:
        repo = RepoSubject(id=repo_id, name=repo_id.split("/")[-1])

    desc = repo.generated_description or ""

    prompt = (
        "in one single and simple 1-4 words phrase/noun phrase, can you say what this is (eg. new aggregator, functional programming language, data workflow orchestration)? use simple and understandable terms Just the answer please\n\n"
        f"{desc}"
    )

    try:
        text = _openrouter_chat(prompt)
    except Exception:
        text = ""

    phrase = (text or "").strip()

    # Persist and mark success (even if empty; minimal enforcement requested)
    repo.kind = phrase
    upsert_repo_subject(conn, repo_id, repo)

    output = ExtractRepoKindOutput(kind=phrase)
    set_work_status(
        conn,
        "extract_repo_kind",
        "repo",
        repo_id,
        "succeeded",
        output.model_dump_json(),
    )
    conn.commit()
    print(f"[task] extract_repo_kind done repo={repo_id} kind_len={len(phrase)}")
