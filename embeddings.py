"""Embedding service for repo and user vectorization using Voyage AI."""
import hashlib
import os
import voyageai
from tenacity import retry, stop_after_attempt, wait_exponential

from models import RepoSubject, UserSubject


# Model configuration
EMBEDDING_MODEL = "voyage-3-large"
EMBEDDING_DIM = 1024


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
)
def get_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """Batch embed multiple texts using Voyage AI.
    
    Args:
        texts: List of text strings to embed (max ~20 for reasonable latency)
    
    Returns:
        List of embedding vectors in same order as input texts
    
    Raises:
        RuntimeError: If API key not configured
        Exception: On API errors
    """
    api_key = os.environ.get("VOYAGE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("VOYAGE_API_KEY not set")
    
    if not texts:
        return []
    
    print(f"[embeddings] Calling Voyage AI API for {len(texts)} texts")
    
    # Initialize Voyage AI client
    vo = voyageai.Client(api_key=api_key)
    
    # Call embedding API with batch of texts
    result = vo.embed(
        texts, 
        model=EMBEDDING_MODEL, 
        input_type="document"
    )
    
    # Extract embeddings from result
    embeddings = result.embeddings
    
    # Validate response
    if len(embeddings) != len(texts):
        raise ValueError(f"Expected {len(texts)} embeddings, got {len(embeddings)}")
    
    for embedding in embeddings:
        if len(embedding) != EMBEDDING_DIM:
            raise ValueError(f"Expected {EMBEDDING_DIM} dimensions, got {len(embedding)}")
    
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
