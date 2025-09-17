"""
Repo analyzer - Carbon copy of proj/scripts/description_generator.py logic
for sophisticated codebase analysis and context building.
"""
import os
import time
import tempfile
import subprocess
import shutil
import pathlib
from typing import Dict, List, Optional, Tuple

# Repo analysis configuration (copied from scripts)
MAX_TOKENS = 5000
PRIORITY_FILES = [
    "README.md", "README", "README.rst",
    "package.json", "requirements.txt", "pyproject.toml",
    "Pipfile", "poetry.lock",
    "go.mod", "Cargo.toml",
    "pom.xml", "build.gradle", "build.gradle.kts",
    "Gemfile", "composer.json",
    "Dockerfile", "docker-compose.yml",
    "Procfile", "fly.toml", "render.yaml",
    "next.config.js", "nuxt.config.*",
    "manage.py", "app.py", "main.py", "server.py",
    "index.js", "server.js", "app.js",
]
PRIORITY_DIRS = [
    "src", "app", "server", "cmd", "internal", "pkg", "lib", "services", "backend", "frontend", "api", "docs"
]
SKIP_DIRS = {".git", ".hg", ".svn", "node_modules", ".next", "dist", "build", "target", ".venv", "venv", "__pycache__", ".terraform", ".gradle", ".idea", ".vscode"}
ALLOWED_EXTS = {".md",".rst",".txt",".py",".ts",".tsx",".js",".jsx",".go",".rs",".java",".kt",".rb",".php",".scala",
                ".c",".h",".cpp",".hpp",".m",".mm",".swift",".cs",".sql",".yaml",".yml",".toml",".ini",".cfg",".json",".graphql",".gql",".sh",".bat",".ps1"}

BLURB_PROMPT = """hey can you give a brief one paragraph description of what this project is, the technologies that it use at how it does it, just brief, for a blurb, but explore the codebase. just give the blurb and nothing else. try to not use adjectives when not necessary, just make it concise, like don't add (intelligent, custom, sophisticated, or things along that line), this should be plain, objective description"""


def safe_read_text(path: pathlib.Path, max_len_head=5000, max_len_tail=2000):
    """Safely read text file with head+tail truncation (copied from scripts)"""
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            content = f.read()
        if len(content) <= max_len_head + max_len_tail:
            return content
        # Truncate: head + "..." + tail
        head = content[:max_len_head]
        tail = content[-max_len_tail:]
        return f"{head}\n... [truncated] ...\n{tail}"
    except Exception:
        return None


def collect_context(repo_dir: pathlib.Path, count_tokens_func) -> Tuple[str, List[str]]:
    """Collect prioritized files for analysis (carbon copy from scripts)"""
    picked = []
    files_included = []

    # 1) Priority files at root
    for name in PRIORITY_FILES:
        for fp in repo_dir.glob(name):
            if fp.is_file():
                picked.append(fp)

    # 2) Priority dirs: walk shallow-ish
    for dname in PRIORITY_DIRS:
        d = repo_dir / dname
        if d.is_dir():
            for root, dirs, files in os.walk(d):
                # prune
                dirs[:] = [dd for dd in dirs if dd not in SKIP_DIRS and not dd.startswith(".git")]
                for f in files:
                    fp = pathlib.Path(root, f)
                    if fp.suffix.lower() in ALLOWED_EXTS:
                        picked.append(fp)
                # keep it light
                if len(picked) > 250:
                    break

    # 3) Fallback: README anywhere if not already picked
    if not any(p.name.lower().startswith("readme") for p in picked):
        for fp in repo_dir.rglob("README*"):
            if fp.is_file():
                picked.append(fp)
                break

    # Dedup while preserving order
    seen = set()
    uniq = []
    for p in picked:
        sp = str(p.resolve())
        if sp not in seen and p.exists():
            uniq.append(p); seen.add(sp)

    # Stitch into context until MAX_TOKENS budget (incremental building)
    parts = ["<BEGIN CONTEXT>"]
    for p in uniq:
        rel = p.relative_to(repo_dir)
        if any(seg in SKIP_DIRS for seg in rel.parts):
            continue
        if p.suffix and p.suffix.lower() not in ALLOWED_EXTS:
            continue
        content = safe_read_text(p)
        if not content:
            continue
        chunk = f"\n### {rel}\n{content}\n"
        # Build candidate context and check token budget
        candidate_context = "\n".join(parts + [chunk] + ["<END CONTEXT>"])
        total_tokens = count_tokens_func(candidate_context + "\n\n" + BLURB_PROMPT)
        if total_tokens > MAX_TOKENS:
            break
        parts.append(chunk)
        files_included.append(str(rel))

    parts.append("<END CONTEXT>")
    return "\n".join(parts), files_included


def clone_and_analyze_repo(owner: str, name: str, count_tokens_func, verbose: bool = False) -> Tuple[str, List[str]]:
    """Clone repo to temp dir and analyze it (carbon copy from scripts)"""
    repo_url = f"https://github.com/{owner}/{name}"
    
    if verbose:
        print(f"  [repo_analyzer] Cloning {repo_url}")
    
    # Create unique temp directory for parallel safety
    timestamp = int(time.time())
    pid = os.getpid()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix=f"repo-desc-{owner}-{name}-{timestamp}-{pid}-"))
    
    try:
        # Shallow clone (same as scripts)
        subprocess.run(
            ["git", "clone", "--depth", "1", repo_url, str(tmp)], 
            check=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            timeout=60  # Don't hang forever
        )

        # If the repo was cloned into a subfolder, adjust (same logic as scripts)
        children = list(tmp.iterdir())
        if len(children) == 1 and children[0].is_dir() and (children[0] / ".git").exists():
            repo_root = children[0]
        else:
            repo_root = tmp

        # Collect and analyze (same as scripts)
        if verbose:
            print(f"  [repo_analyzer] Analyzing repository context...")
        
        context, files_included = collect_context(repo_root, count_tokens_func)
        
        if verbose:
            print(f"  [repo_analyzer] Analyzed {len(files_included)} files")
        
        return context, files_included
        
    finally:
        # Always cleanup temp directory
        shutil.rmtree(tmp, ignore_errors=True)
