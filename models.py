"""Pydantic models for data validation and serialization.

All JSON data stored in the database should be validated through these models.
"""
from typing import Optional, Literal
from pydantic import BaseModel, Field


# -------------------- Type Aliases --------------------

ItemType = Literal["repo", "trending_repo", "user", "hackernews"]


# -------------------- Gallery Image --------------------


class GalleryImage(BaseModel):
    """Image in a repository's gallery."""

    url: str
    alt: str = ""
    original_url: str
    title: str = ""
    caption: str = ""
    is_highlight: bool = True
    taken_at: int = 0  # epoch milliseconds, 0 if unknown


# -------------------- Subject Models --------------------


class UserSubject(BaseModel):
    """User subject stored in subjects table (subject_type='user').

    Represents a GitHub user profile with additional computed metadata.
    """

    login: str
    avatar_url: Optional[str] = None
    bio: Optional[str] = None
    location: Optional[str] = None
    blog: Optional[str] = None
    theme: Optional[str] = None
    highlighted_repos: list[str] = Field(default_factory=list)
    is_ghost: bool = False


class ForYouUserItem(UserSubject):
    """User item in the for-you-users feed."""
    pass  # Inherits all fields including is_ghost


class RepoSubject(BaseModel):
    """Repository subject stored in subjects table (subject_type='repo').

    Represents a GitHub repository with enriched metadata from various tasks.
    """

    # Core fields (from GitHub API)
    id: str  # full_name: "owner/repo"
    name: str
    description: Optional[str] = None
    language: Optional[str] = None
    stargazers_count: int = 0
    fork: bool = False
    pushed_at: Optional[str] = None
    updated_at: Optional[str] = None
    topics: list[str] = Field(default_factory=list)

    # Metadata fields
    extracted_at: Optional[
        str
    ] = None  # ISO timestamp of when we scraped/fetched this data

    # Enhanced fields (added by tasks)
    link: Optional[str] = None  # from enhance_repo_media
    gallery: list[GalleryImage] = Field(default_factory=list)  # from enhance_repo_media
    generated_description: Optional[str] = None  # from generate_repo_blurb
    emphasis: Optional[list[str]] = None  # from extract_repo_emphasis
    keywords: Optional[list[str]] = None  # from extract_repo_keywords
    kind: Optional[str] = None  # from extract_repo_kind


class ForYouRepoItem(RepoSubject):
    """Repository item in the for-you feed, includes repo data plus metadata."""

    username: str  # The user this repo is associated with
    is_ghost: bool = False  # Whether the associated user is a ghost


class HackernewsSubject(BaseModel):
    """HN story stored in subjects table (subject_type='hackernews').
    
    Represents a Hacker News story with metadata.
    """
    
    id: str  # HN story ID (e.g., "45631503")
    title: str
    url: Optional[str] = None  # External article URL (None for Ask HN, etc.)
    by: str  # HN username
    score: int = 0
    time: int  # Unix timestamp (seconds)
    descendants: int = 0  # Number of comments
    extracted_at: str  # ISO timestamp of when we fetched it


class ForYouHackernewsItem(HackernewsSubject):
    """HN item in the for-you feed."""
    
    hn_url: str  # HN discussion URL: f"https://news.ycombinator.com/item?id={id}"


# -------------------- Work Item Output Models --------------------


class FetchProfileOutput(BaseModel):
    """Output from fetch_profile task."""

    profile_found: bool


class FetchReposOutput(BaseModel):
    """Output from fetch_repos task."""

    fetched: int


class SelectHighlightedReposOutput(BaseModel):
    """Output from select_highlighted_repos task."""

    count: int


class InferUserThemeOutput(BaseModel):
    """Output from infer_user_theme task."""

    theme: str


class EnhanceRepoMediaOutput(BaseModel):
    """Output from enhance_repo_media task."""

    link: Optional[str] = None
    gallery_count: int
    added: int


class GenerateRepoBlurbOutput(BaseModel):
    """Output from generate_repo_blurb task."""

    description_len: int


class ExtractRepoEmphasisOutput(BaseModel):
    """Output from extract_repo_emphasis task."""

    emphasis: list[str]
    count: int


class ExtractRepoKeywordsOutput(BaseModel):
    """Output from extract_repo_keywords task."""

    keywords: list[str]
    count: int


class ExtractRepoKindOutput(BaseModel):
    """Output from extract_repo_kind task."""

    kind: str


# -------------------- Recommendation Model --------------------


class Recommendation(BaseModel):
    """Recommendation row from recommendations table."""
    
    user_id: str
    item_type: ItemType
    item_id: str
    times_shown: int = 0
    last_shown_at: Optional[float] = None  # epoch seconds


class WorkItem(BaseModel):
    """Work item row from work_items table."""

    id: str
    kind: str
    subject_type: str
    subject_id: str
    status: str  # "pending" | "running" | "succeeded" | "failed"
    output_json: Optional[str] = None
    processed_at: Optional[float] = None


# -------------------- User Context Model --------------------


class UserContext(BaseModel):
    """User-provided context stored in user_contexts table.
    
    User contexts allow users to provide additional information about their interests,
    current projects, or goals that influence their feed recommendations.
    Each context gets its own embedding and contributes to feed ranking via max similarity.
    """
    
    id: str  # Format: 'username:uuid'
    user_id: str  # GitHub username
    content: str  # User-provided text content
    embedding: Optional[list[float]] = None  # 1024-dim Voyage AI embedding
    created_at: float  # Unix timestamp
    updated_at: float  # Unix timestamp
