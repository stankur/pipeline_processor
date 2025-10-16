"""Embedding service for repo and user vectorization using DeepInfra API."""
import hashlib
import os
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from models import RepoSubject, UserSubject


# Model configuration
EMBEDDING_MODEL = "Qwen/Qwen3-Embedding-8B"
EMBEDDING_DIM = 4096
API_ENDPOINT = "https://api.deepinfra.com/v1/openai/embeddings"


def format_repo_for_embedding(repo: RepoSubject) -> str:
    """Format repo data for embedding (matches LLM prompt format).
    
    Returns text in format:
    {name}
    {description}
    {generated_description}
    {language}
    
    Maximum content, minimal noise - no labels.
    """
    parts = []
    
    if repo.name:
        parts.append(repo.name)
    
    if repo.description:
        parts.append(repo.description.strip())
    
    if repo.generated_description:
        parts.append(repo.generated_description.strip())
    
    if repo.language:
        parts.append(repo.language)
    
    return "\n".join(parts)


def format_user_for_embedding(user: UserSubject, highlighted_repos: list[RepoSubject]) -> str:
    """Format user profile for embedding (matches LLM prompt format).
    
    Returns text in format:
    {username}
    {bio}
    
    user's highlighted repositories:
    {repo1_name}
    {repo1_description}
    {repo1_generated_description}
    {repo1_language}
    
    {repo2_name}
    ...
    
    Maximum content, minimal noise - no labels.
    """
    parts = []
    
    # User info
    if user.login:
        parts.append(user.login)
    
    if user.bio:
        parts.append(user.bio.strip())
    
    # Highlighted repos section
    if highlighted_repos:
        parts.append("")  # Empty line separator
        parts.append("user's highlighted repositories:")
        
        for repo in highlighted_repos:
            repo_parts = []
            if repo.name:
                repo_parts.append(repo.name)
            if repo.description:
                repo_parts.append(repo.description.strip())
            if repo.generated_description:
                repo_parts.append(repo.generated_description.strip())
            if repo.language:
                repo_parts.append(repo.language)
            
            if repo_parts:
                parts.append("\n".join(repo_parts))
                parts.append("")  # Empty line between repos
    
    return "\n".join(parts)


def compute_embedding_hash(text: str) -> str:
    """Compute SHA-256 hash of text for change detection."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.RequestException, ValueError))
)
def get_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """Batch embed multiple texts using DeepInfra API.
    
    Args:
        texts: List of text strings to embed (max ~5-10 for reasonable latency)
    
    Returns:
        List of embedding vectors in same order as input texts
    
    Raises:
        RuntimeError: If API key not configured
        requests.HTTPError: On API errors
        ValueError: If response format invalid
    """
    api_key = os.environ.get("DEEPINFRA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DEEPINFRA_API_KEY not set")
    
    if not texts:
        return []
    
    print(f"[embeddings] Calling DeepInfra API for {len(texts)} texts")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    data = {
        "model": EMBEDDING_MODEL,
        "input": texts,
        "encoding_format": "float",
    }
    
    response = requests.post(API_ENDPOINT, headers=headers, json=data, timeout=60)
    
    if not response.ok:
        print(f"[embeddings] DeepInfra error {response.status_code}: {response.text}")
    response.raise_for_status()
    
    result = response.json()
    
    # Extract embeddings in order
    data_items = result.get("data", [])
    if len(data_items) != len(texts):
        raise ValueError(f"Expected {len(texts)} embeddings, got {len(data_items)}")
    
    # Sort by index to ensure correct order
    data_items.sort(key=lambda x: x.get("index", 0))
    
    embeddings = []
    for item in data_items:
        embedding = item.get("embedding")
        if not embedding or not isinstance(embedding, list):
            raise ValueError("Invalid embedding format in response")
        if len(embedding) != EMBEDDING_DIM:
            raise ValueError(f"Expected {EMBEDDING_DIM} dimensions, got {len(embedding)}")
        embeddings.append(embedding)
    
    print(f"[embeddings] Successfully embedded {len(embeddings)} texts")
    return embeddings


def get_embedding(text: str) -> list[float]:
    """Embed a single text (convenience wrapper around batch function).
    
    Args:
        text: Text string to embed
    
    Returns:
        Embedding vector of dimension EMBEDDING_DIM
    """
    embeddings = get_embeddings_batch([text])
    return embeddings[0]

