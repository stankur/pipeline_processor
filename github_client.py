import json
import os
import time

import requests
from dotenv import load_dotenv

GITHUB_API = "https://api.github.com"
DEFAULT_USER_AGENT = "net-github-scripts/1.0"


class GitHubClient:
    """Minimal GitHub REST v3 client with retry/backoff for rate limits."""

    def __init__(self, token: str | None = None, user_agent: str | None = None):
        # Load .env lazily the first time a client is created
        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=False)
        self.token = token or os.environ.get("GITHUB_TOKEN", "").strip()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "User-Agent": user_agent or DEFAULT_USER_AGENT,
            }
        )
        if self.token:
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def _sleep_until_reset(self, reset_header: str | None) -> None:
        try:
            reset_epoch = int(reset_header) if reset_header else 0
        except Exception:
            reset_epoch = 0
        now = int(time.time())
        wait_seconds = max(1, reset_epoch - now + 1) if reset_epoch else 10
        time.sleep(min(wait_seconds, 60))

    def _request(self, method: str, url: str, **kwargs):
        while True:
            resp = self.session.request(method, url, **kwargs)
            if resp.status_code == 403 and (
                "rate limit" in resp.text.lower() or resp.headers.get("X-RateLimit-Remaining") == "0"
            ):
                self._sleep_until_reset(resp.headers.get("X-RateLimit-Reset"))
                continue
            if resp.status_code >= 400:
                resp.raise_for_status()
            return resp

    def get_json(self, url: str, params: dict | None = None):
        resp = self._request("GET", url, params=params)
        return resp.json()

    # -------- Users ---------
    def get_user(self, login: str) -> dict:
        return self.get_json(f"{GITHUB_API}/users/{login}")

    def _list_edges(self, login: str, kind: str, limit: int | None = None) -> list[str]:
        assert kind in {"followers", "following"}
        per_page = 100
        results: list[str] = []
        page = 1
        while True:
            if limit is not None and len(results) >= limit:
                break
            params = {"per_page": per_page, "page": page}
            data = self.get_json(f"{GITHUB_API}/users/{login}/{kind}", params)
            if not data:
                break
            results.extend([u.get("login") for u in data if u.get("login")])
            if len(data) < per_page:
                break
            page += 1
        return results[: limit or None]

    def list_followers(self, login: str, limit: int | None = None) -> list[str]:
        return self._list_edges(login, "followers", limit)

    def list_following(self, login: str, limit: int | None = None) -> list[str]:
        return self._list_edges(login, "following", limit)

    # -------- Repos ---------
    def list_repos(
        self,
        login: str,
        sort: str = "pushed",
        per_page: int = 100,
        limit: int | None = None,
    ) -> list[dict]:
        assert per_page <= 100
        results: list[dict] = []
        page = 1
        while True:
            if limit is not None and len(results) >= limit:
                break
            params = {"per_page": per_page, "page": page, "sort": sort}
            data = self.get_json(f"{GITHUB_API}/users/{login}/repos", params)
            if not data:
                break
            results.extend(data)
            if len(data) < per_page:
                break
            page += 1
        return results[: limit or None]

    def get_repo(self, owner: str, repo: str) -> dict:
        """Get detailed repository information including parent for forks."""
        return self.get_json(f"{GITHUB_API}/repos/{owner}/{repo}")

    def get_rate_limit(self) -> dict:
        """Get current rate limit status."""
        return self.get_json(f"{GITHUB_API}/rate_limit")

    def get_repo_readme(self, owner: str, repo: str) -> tuple[str | None, dict | None]:
        meta_url = f"{GITHUB_API}/repos/{owner}/{repo}/readme"
        try:
            meta = self.get_json(meta_url)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None, None
            raise
        download_url = meta.get("download_url")
        if not download_url:
            resp = self._request(
                "GET",
                meta_url,
                headers={"Accept": "application/vnd.github.v3.raw"},
            )
            content = resp.text
            return content, meta
        resp = self._request("GET", download_url)
        return resp.text, meta


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, data) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_csv(path: str, rows: list[dict], fieldnames: list[str]) -> None:
    import csv

    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


def normalize_repo_fields(repo: dict) -> dict:
    return {
        "name": repo.get("name"),
        "description": repo.get("description"),
        "fork": repo.get("fork"),
        "stargazers_count": repo.get("stargazers_count"),
        "created_at": repo.get("created_at"),
        "pushed_at": repo.get("pushed_at"),
        "html_url": repo.get("html_url"),
    }

