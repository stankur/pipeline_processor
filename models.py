"""Pydantic models for data validation and serialization.

All JSON data stored in the database should be validated through these models.
"""
from typing import Optional, Literal
from pydantic import BaseModel, Field


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

