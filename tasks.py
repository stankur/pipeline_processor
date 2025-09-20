import json
from datetime import datetime, timezone, timedelta

import os
import time
import json
from typing import Any, Dict, List, Optional

import yaml
import tiktoken
from dotenv import load_dotenv

from db import get_conn, upsert_subject, set_work_status, get_subject, get_user_repos
from github_client import GitHubClient

from logging import getLogger

logger = getLogger(__name__)


def _unique_commit_days(commits: list[dict]) -> int:
    days = set()
    for c in commits:
        try:
            meta = c.get("commit", {})
            date_str = meta.get("author", {}).get("date") or meta.get("committer", {}).get("date")
            if date_str:
                days.add(date_str.split("T")[0])
        except Exception:
            continue
    return len(days)


def _recent_enough(ts: str | None, years: int = 2) -> bool:
    if not ts:
        return True  # be permissive if missing
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt >= (datetime.now(timezone.utc) - timedelta(days=years * 365))
    except Exception:
        return True


def fetch_profile(username: str) -> None:
    """Fetch GitHub profile JSON for username and store it under subjects('user', username).

    Also marks the corresponding work_item status to 'running'->'succeeded' with a tiny summary.
    """
    print(f"[task] fetch_profile start username={username}")
    conn = get_conn()
    set_work_status(conn, "fetch_profile", "user", username, "running")
    client = GitHubClient()
    profile = client.get_user(username)
    print(f"[task] fetch_profile got profile login={profile.get('login')}")
    # Keep only lean profile fields
    lean = {
        "login": profile.get("login"),
        "avatar_url": profile.get("avatar_url"),
        "bio": profile.get("bio"),
        "location": profile.get("location"),
        "blog": profile.get("blog"),
    }
    upsert_subject(conn, "user", username, json.dumps(lean))
    print(f"[task] fetch_profile upserted username={username}")
    set_work_status(conn, "fetch_profile", "user", username, "succeeded", json.dumps({"profile_found": True}))
    conn.commit()
    print(f"[task] fetch_profile commit username={username}")
    print(f"[task] fetch_profile done username={username}")


def fetch_repos(username: str) -> None:
    """Fetch and filter repositories for username; write only kept repos as lean subjects('repo','user/repo')."""
    print(f"[task] fetch_repos start username={username}")
    conn = get_conn()
    set_work_status(conn, "fetch_repos", "user", username, "running")
    client = GitHubClient()

    # Fetch recent repos (cap at 30 to limit API calls)
    repos = client.list_repos(username, sort="pushed", per_page=30, limit=30)
    print(f"[task] fetch_repos fetched count={len(repos)}")

    kept: list[dict] = []

    for repo in repos:
        name = repo.get("name")
        full_name = repo.get("full_name") or f"{username}/{name}"
        # language
        if not repo.get("language"):
            continue
        # recency (2 years)
        pushed = repo.get("pushed_at") or repo.get("updated_at")
        if not _recent_enough(pushed, years=2):
            continue
        # non-fork for kept set
        if repo.get("fork"):
            continue

        kept.append({
            "id": full_name,
            "name": name,
            "description": repo.get("description"),
            "language": repo.get("language"),
            "stargazers_count": repo.get("stargazers_count", 0),
            "fork": False,
            "pushed_at": repo.get("pushed_at"),
            "updated_at": repo.get("updated_at"),
            "topics": repo.get("topics", []),
        })

    print(f"[task] fetch_repos kept count={len(kept)}")

    # Upsert only kept repos
    for r in kept:
        upsert_subject(conn, "repo", r["id"], json.dumps(r))

    set_work_status(conn, "fetch_repos", "user", username, "succeeded", json.dumps({"fetched": len(kept)}))
    conn.commit()
    print(f"[task] fetch_repos commit username={username}")
    print(f"[task] fetch_repos done username={username} kept={len(kept)}")


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
    resp = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=data, timeout=60)
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
    set_work_status(conn, "select_highlighted_repos", "user", username, "running")

    rows = get_user_repos(conn, username)
    repos: List[Dict[str, Any]] = []
    for r in rows:
        try:
            obj = json.loads(r["data_json"]) if r["data_json"] else None
            if obj:
                repos.append(obj)
        except Exception:
            continue

    # Log kept repos overview
    try:
        kept_names = [r.get("name", "") for r in repos]
        print(f"[task] select_highlighted_repos kept_repos count={len(kept_names)} names={kept_names}")
    except Exception:
        pass

    # Filter to strict criteria (README + 4 unique commit days) for highlight selection
    client = GitHubClient()
    strict: List[Dict[str, Any]] = []
    for r in repos:
        try:
            name = r.get("name")
            if not name:
                continue
            readme_content, _ = client.get_repo_readme(username, name)
            if not readme_content:
                continue
            commits = client.get_json(
                f"https://api.github.com/repos/{username}/{name}/commits",
                params={"per_page": 100},
            )
            if _unique_commit_days(commits) < 4:
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
        print(f"[task] select_highlighted_repos PROMPT BEGIN\n{full_prompt}\nPROMPT END")

        if count_tokens(full_prompt) < 5000:
            print(f"[task] select_highlighted_repos token count OK, calling LLM")
            try:
                text = _openrouter_chat(full_prompt)
                print(f"[task] select_highlighted_repos LLM returned: {bool(text)} (length={len(text) if text else 0})")
                if text:
                    # Log raw response
                    print(f"[task] select_highlighted_repos RESPONSE BEGIN\n{text}\nRESPONSE END")
                    data = json.loads(text)
                    print(f"[task] select_highlighted_repos JSON parsed: {data}")
                    rr = data.get("repos") if isinstance(data, dict) else None
                    if isinstance(rr, list):
                        names = [str(x).strip() for x in rr if str(x).strip()]
                        print(f"[task] select_highlighted_repos extracted repo names: {names}")
                    else:
                        print(f"[task] select_highlighted_repos repos field is not a list: {rr}")
                else:
                    print(f"[task] select_highlighted_repos LLM returned empty response")
            except Exception as e:
                print(f"[task] select_highlighted_repos ERROR: {e}")
                names = []
        else:
            print(f"[task] select_highlighted_repos token count too high: {count_tokens(full_prompt)} >= 5000")

    out = {"repos": names}
    print(f"[task] select_highlighted_repos parsed repos count={len(names)} names={names}")
    set_work_status(conn, "select_highlighted_repos", "user", username, "succeeded", json.dumps(out))
    conn.commit()
    print(f"[task] select_highlighted_repos done username={username} count={len(names)}")


def infer_user_theme(username: str) -> None:
    """Infer a user theme using only the highlighted repos.

    Stores output_json: {"theme": str}. Also writes theme + highlighted_repos into user subject.
    """
    print(f"[task] infer_user_theme start username={username}")
    conn = get_conn()
    set_work_status(conn, "infer_user_theme", "user", username, "running")

    # Read highlighted repos from previous task
    row = conn.execute(
        "SELECT output_json FROM work_items WHERE kind='select_highlighted_repos' AND subject_type='user' AND subject_id=? AND status='succeeded'",
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

    print(f"[task] infer_user_theme highlights count={len(highlights)} names={highlights}")

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
                print(f"[task] infer_user_theme LLM returned: {bool(text)} (length={len(text) if text else 0})")
                if text:
                    print(f"[task] infer_user_theme RESPONSE BEGIN\n{text}\nRESPONSE END")
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
            print(f"[task] infer_user_theme token count too high: {count_tokens(full_prompt)} >= 5000")

    # Persist into user subject
    urow = get_subject(conn, "user", username)
    ubase = {}
    if urow and urow["data_json"]:
        try:
            ubase = json.loads(urow["data_json"]) or {}
        except Exception:
            ubase = {}
    ubase["theme"] = theme
    ubase["highlighted_repos"] = highlights
    upsert_subject(conn, "user", username, json.dumps(ubase))

    print(f"[task] infer_user_theme parsed theme_len={len(theme)} theme={theme}")
    set_work_status(conn, "infer_user_theme", "user", username, "succeeded", json.dumps({"theme": theme}))
    conn.commit()
    print(f"[task] infer_user_theme done username={username} theme_len={len(theme)} highlights={len(highlights)}")


def enhance_repo_media(repo_id: str) -> None:
    """Enhance a single repo subject with link and gallery extracted from README.

    repo_id: "user/repo"
    """
    print(f"[task] enhance_repo_media start repo={repo_id}")
    conn = get_conn()
    set_work_status(conn, "enhance_repo_media", "repo", repo_id, "running")

    try:
        owner, name = repo_id.split("/", 1)
    except ValueError:
        set_work_status(conn, "enhance_repo_media", "repo", repo_id, "failed")
        conn.commit()
        return

    client = GitHubClient()

    # Fetch README content and repo metadata
    readme_content, _ = client.get_repo_readme(owner, name)
    repo_meta = None
    try:
        repo_meta = client.get_repo(owner, name)
    except Exception:
        repo_meta = None

    # Link
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

    # Default branch
    default_branch = "main"
    if repo_meta and isinstance(repo_meta.get("default_branch"), str):
        default_branch = repo_meta.get("default_branch") or "main"

    # Gallery: simple markdown image extractor
    import re

    gallery: List[Dict[str, str]] = []
    if readme_content:
        pattern = r"!\[([^\]]*)\]\(([^)]+)\)"
        for match in re.findall(pattern, readme_content):
            alt_text, url = match
            u = (url or "").strip()
            lower = u.lower()
            # Skip badges/videos
            if any(x in lower for x in ["shields.io", "badge", "actions/workflows", ".mp4", ".webm", ".mov"]):
                continue
            # Normalize to absolute raw URL when relative
            if not (u.startswith("http://") or u.startswith("https://")):
                u = f"https://github.com/{owner}/{name}/raw/{default_branch}/{u.lstrip('./').lstrip('/')}"
            gallery.append({"alt": alt_text.strip(), "url": u, "original_url": url})

    # Merge into repo subject
    row = get_subject(conn, "repo", repo_id)
    base = {}
    if row and row["data_json"]:
        try:
            base = json.loads(row["data_json"]) or {}
        except Exception:
            base = {}
    base["link"] = link
    base["gallery"] = gallery
    upsert_subject(conn, "repo", repo_id, json.dumps(base))

    set_work_status(conn, "enhance_repo_media", "repo", repo_id, "succeeded", json.dumps({"link": link, "gallery_count": len(gallery)}))
    conn.commit()
    print(f"[task] enhance_repo_media done repo={repo_id} link={bool(link)} gallery={len(gallery)}")


def generate_repo_blurb(repo_id: str) -> None:
    """Generate a concise LLM blurb for a repo using sophisticated codebase analysis.

    Uses the same logic as proj/scripts for actual codebase exploration.
    Stores under repo subject field 'generated_description'.
    """
    print(f"[task] generate_repo_blurb start repo={repo_id}")
    conn = get_conn()
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
        context, files_included = clone_and_analyze_repo(owner, name, count_tokens, verbose=True)
        full_prompt = f"{context}\n\n{BLURB_PROMPT}"
        
        print(f"[task] generate_repo_blurb analyzed {len(files_included)} files: {files_included}")
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
    row = get_subject(conn, "repo", repo_id)
    base = {}
    if row and row["data_json"]:
        try:
            base = json.loads(row["data_json"]) or {}
        except Exception:
            base = {}
    if desc:
        base["generated_description"] = desc
        upsert_subject(conn, "repo", repo_id, json.dumps(base))

    set_work_status(conn, "generate_repo_blurb", "repo", repo_id, "succeeded", json.dumps({"generated": bool(desc)}))
    conn.commit()
    print(f"[task] generate_repo_blurb done repo={repo_id} has_desc={bool(desc)}")


# -------------------- New task: extract emphasis from generated_description --------------------

def extract_repo_emphasis(repo_id: str) -> None:
    """Extract technology emphasis array from generated_description and store it on the repo subject.

    Fails fast if preconditions or parsing fail; succeeds only when emphasis is extracted and saved.
    """
    print(f"[task] extract_repo_emphasis start repo={repo_id}")
    conn = get_conn()
    set_work_status(conn, "extract_repo_emphasis", "repo", repo_id, "running")

    # Load repo subject
    row = get_subject(conn, "repo", repo_id)
    if not row or not row["data_json"]:
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

    try:
        base = json.loads(row["data_json"]) or {}
    except Exception:
        set_work_status(
            conn,
            "extract_repo_emphasis",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": "invalid_subject_json"}),
        )
        conn.commit()
        print(f"[task] extract_repo_emphasis FAILED repo={repo_id} reason=invalid_subject_json")
        return

    desc = base.get("generated_description")
    if not isinstance(desc, str) or not desc.strip():
        set_work_status(
            conn,
            "extract_repo_emphasis",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": "no_generated_description"}),
        )
        conn.commit()
        print(f"[task] extract_repo_emphasis FAILED repo={repo_id} reason=no_generated_description")
        return

    prompt = (
        "can you extract an array parseable with json.loads, of the frameworks that this project is dependent on, "
        "languages, technologies, or libraries used. Just the array and nothing else, make the capitalization exactly as written:\n\n"
        f"{desc}"
    )

    # Call LLM and parse strictly
    try:
        text = _openrouter_chat(prompt)
        if not text:
            raise ValueError("empty_llm_response")
        data = json.loads(text)
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
    base["emphasis"] = emphasis_list
    upsert_subject(conn, "repo", repo_id, json.dumps(base))
    set_work_status(
        conn,
        "extract_repo_emphasis",
        "repo",
        repo_id,
        "succeeded",
        json.dumps({"extracted": True, "count": len(emphasis_list)}),
    )
    conn.commit()
    print(f"[task] extract_repo_emphasis done repo={repo_id} count={len(emphasis_list)}")


# -------------------- New task: extract keywords (skills) from generated_description --------------------

def extract_repo_keywords(repo_id: str) -> None:
    """Extract 1-4 conceptual skill keywords from generated_description and store on repo subject.

    Fails fast if preconditions or parsing fail; succeeds only when keywords are extracted and saved.
    """
    print(f"[task] extract_repo_keywords start repo={repo_id}")
    conn = get_conn()
    set_work_status(conn, "extract_repo_keywords", "repo", repo_id, "running")

    # Load repo subject
    row = get_subject(conn, "repo", repo_id)
    if not row or not row["data_json"]:
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

    try:
        base = json.loads(row["data_json"]) or {}
    except Exception:
        set_work_status(
            conn,
            "extract_repo_keywords",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": "invalid_subject_json"}),
        )
        conn.commit()
        print(f"[task] extract_repo_keywords FAILED repo={repo_id} reason=invalid_subject_json")
        return

    desc = base.get("generated_description")
    if not isinstance(desc, str) or not desc.strip():
        set_work_status(
            conn,
            "extract_repo_keywords",
            "repo",
            repo_id,
            "failed",
            json.dumps({"reason": "no_generated_description"}),
        )
        conn.commit()
        print(f"[task] extract_repo_keywords FAILED repo={repo_id} reason=no_generated_description")
        return

    prompt = (
        "can you extract the main keywords that represent the technical techniques/knowledge/concepts/interest displayed in this project, "
        "and output an array of 1-4 keywords, that will be used to display the skills of the person (don't list tools/frameworks/programming language):\n\n"
        f"{desc}"
    )

    # Call LLM and parse strictly
    try:
        text = _openrouter_chat(prompt)
        if not text:
            raise ValueError("empty_llm_response")
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError("not_array")
        if not (1 <= len(data) <= 4):
            raise ValueError("array_length_out_of_range")
        if not all(isinstance(x, str) and x.strip() for x in data):
            raise ValueError("invalid_array_elements")
        keywords_list = [x.strip() for x in data]
    except Exception as e:
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
    base["keywords"] = keywords_list
    upsert_subject(conn, "repo", repo_id, json.dumps(base))
    set_work_status(
        conn,
        "extract_repo_keywords",
        "repo",
        repo_id,
        "succeeded",
        json.dumps({"extracted": True, "count": len(keywords_list)}),
    )
    conn.commit()
    print(f"[task] extract_repo_keywords done repo={repo_id} count={len(keywords_list)}")

