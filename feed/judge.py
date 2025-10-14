"""LLM judgment with caching and fingerprinting."""
from __future__ import annotations
import hashlib
import json
import os
from typing import Any, Dict
import yaml
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from psycopg import Connection
from utils import parse_llm_json
from feed.serialize import serialize_user_profile_minimal, serialize_repo_for_llm
from db import get_recommendation, upsert_recommendation_judgment
from models import UserSubject, RepoSubject, ItemType


# LLM Configuration
MODEL_NAME = "google/gemini-2.5-pro"
TEMPERATURE = 0.1


def _load_prompts() -> Dict[str, Any]:
    """Load prompts from prompt.yaml in the parent directory."""
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "prompt.yaml"))
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


# Load prompts from YAML
_prompts = _load_prompts()
USER_PROMPT_TEMPLATE = _prompts.get("for_you_user_prompt", "")
PROMPT_TEMPLATE = _prompts.get("for_you_full_prompt", "")


def render_user_prompt(user_payload: dict) -> str:
    """Render the user portion of the prompt.
    
    Single source of truth for user prompt formatting.
    Used in both fingerprinting and full prompt rendering.
    """
    # Format highlights as unordered list with details
    highlights_list = []
    for h in user_payload.get("highlights", []):
        lines = [f"- {h['name']}"]
        if h.get("description"):
            lines.append(f"  {h['description']}")
        if h.get("generated_description"):
            lines.append(f"  {h['generated_description']}")
        if h.get("language"):
            lines.append(f"  {h['language']}")
        highlights_list.append("\n".join(lines))
    
    highlights_str = "\n\n".join(highlights_list) if highlights_list else "(none)"
    
    return USER_PROMPT_TEMPLATE.format(
        username=user_payload.get("username", ""),
        bio=user_payload.get("bio", ""),
        highlights=highlights_str,
    )


def make_judgment_fingerprint(conn: Connection, user: UserSubject) -> str:
    """Compute SHA-256 hash of judgment inputs (user prompt + model config).
    
    This captures:
    - User data and how it's formatted in the prompt
    - Model name and temperature settings
    
    Any change to user profile, prompt template, or model config invalidates cache.
    """
    payload = serialize_user_profile_minimal(conn, user)
    user_prompt = render_user_prompt(payload)
    model_config = f"model:{MODEL_NAME},temp:{TEMPERATURE}"
    combined = f"{user_prompt}\n---MODEL---\n{model_config}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def render_prompt(user_payload: dict, item_payload: dict) -> str:
    """Render full prompt from user and item payloads."""
    user_prompt = render_user_prompt(user_payload)
    
    return PROMPT_TEMPLATE.format(
        user_prompt=user_prompt,
        name=item_payload.get("name", ""),
        author=item_payload.get("author", ""),
        description=item_payload.get("description", ""),
        generated_description=item_payload.get("generated_description", ""),
        language=item_payload.get("language", ""),
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.RequestException, ValueError))
)
def llm_boolean_gate(prompt_text: str) -> bool:
    """Call OpenRouter LLM and extract boolean include decision.
    
    
    Raises RuntimeError if API key not configured.
    Raises requests.HTTPError on API errors.
    Raises ValueError if JSON parsing fails.
    """
    api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")
    
    print(f"[judge] === LLM PROMPT START ===")
    print(prompt_text)
    print(f"[judge] === LLM PROMPT END ===")
    print(f"[judge] calling OpenRouter with model={MODEL_NAME}, temp={TEMPERATURE}")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    data = {
        "model": MODEL_NAME,
        "messages": [{"role": "user", "content": prompt_text}],
        "temperature": TEMPERATURE,
    }
    resp = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=data, timeout=120)
    
    if not resp.ok:
        print(f"[judge] OpenRouter error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    
    j = resp.json()
    content = (j.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()
    print(f"[judge] LLM raw response: {content}")
    result = parse_llm_json(content)
    include = bool(result.get("include", False))
    print(f"[judge] LLM parsed result: include={include}")
    return include


def judge_repo_for_user(
    conn: Connection,
    viewer_user: UserSubject,
    repo: RepoSubject,
    author_username: str,
    item_type: ItemType = "repo",
) -> bool:
    """Judge repo for user: get cached judgment or compute new one via LLM.
    
    Recomputes if:
    - No cached judgment exists, OR
    - user_fingerprint changed (profile/highlights updated), OR
    - prompt_hash changed (prompt template updated)
    
    Preserves exposure counters (times_shown, last_shown_at) across re-judgments.
    
    Returns:
        bool: True if item should be included in feed, False otherwise
    """
    viewer_username = viewer_user.login
    item_id = repo.id
    rec = get_recommendation(conn, viewer_username, item_type, item_id)
    judgment_fp = make_judgment_fingerprint(conn, viewer_user)
    
    # Check if we can reuse cached judgment
    reuse = (
        rec is not None
        and rec.include is not None
        and rec.judgment_fingerprint == judgment_fp
    )
    
    if reuse:
        print(f"[judge] cache hit user={viewer_username} item={item_id} include={rec.include}")
        return bool(rec.include)
    
    # Compute fresh judgment
    print(f"[judge] cache miss user={viewer_username} item={item_id}, calling LLM")
    user_payload = serialize_user_profile_minimal(conn, viewer_user)
    item_payload = serialize_repo_for_llm(repo, author_username)
    prompt = render_prompt(user_payload, item_payload)
    
    include = llm_boolean_gate(prompt)
    
    # Persist judgment (preserves exposure counters via ON CONFLICT UPDATE)
    upsert_recommendation_judgment(
        conn, viewer_username, item_type, item_id, include, judgment_fp
    )
    
    return include

