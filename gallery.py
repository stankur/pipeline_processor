"""Gallery functionality for displaying highlighted repos with images."""
from psycopg import Connection
from db import get_all_highlighted_repos_with_gallery, get_user_subject
from models import RepoSubject


def get_gallery_repos(conn: Connection, limit: int = 30) -> list[dict]:
    """Get gallery of highlighted repos with images, sorted by recency.
    
    Args:
        conn: Database connection
        limit: Maximum number of repos to return (default 30)
    
    Returns:
        List of dicts ready for API response, each containing:
        - All RepoSubject fields
        - username: author who highlighted the repo
        - is_ghost: whether the author is a ghost user
    """
    print(f"[gallery] get_gallery_repos: limit={limit}")
    
    # Get all highlighted repos with gallery images
    repos_data = get_all_highlighted_repos_with_gallery(conn)
    
    # Sort by GitHub timestamp descending (most recent first)
    repos_data.sort(key=lambda r: r["github_timestamp"], reverse=True)
    
    # Take top N
    top_repos = repos_data[:limit]
    
    # Format for API response
    results = []
    for item in top_repos:
        repo: RepoSubject = item["repo"]
        author_username: str = item["author_username"]
        
        # Check if author is a ghost user
        author_user = get_user_subject(conn, author_username)
        is_ghost = author_user.is_ghost if author_user else False
        
        # Convert to dict with additional metadata
        repo_dict = repo.model_dump()
        repo_dict["username"] = author_username
        repo_dict["is_ghost"] = is_ghost
        
        results.append(repo_dict)
    
    print(f"[gallery] Returning {len(results)} repos")
    return results

