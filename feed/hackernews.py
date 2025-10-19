"""Fetch and store Hacker News top stories."""
from __future__ import annotations
import requests
from typing import List, Dict, Any
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg import Connection

from models import HackernewsSubject
from db import upsert_hackernews_subject


def fetch_hn_story_details(story_id: int) -> Dict[str, Any] | None:
    """Fetch single story details from HN API.
    
    Args:
        story_id: HN story ID
    
    Returns:
        Story dict if successful, None otherwise
    """
    try:
        resp = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json",
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[hackernews] Failed to fetch story {story_id}: {e}")
        return None


def fetch_hn_top_stories(limit: int = 100) -> List[Dict[str, Any]]:
    """Fetch top N stories from HN API (parallel).
    
    Args:
        limit: Number of top stories to fetch
    
    Returns:
        List of story dicts
    """
    print(f"[hackernews] Fetching top {limit} stories from HN API")
    
    # Get top story IDs
    try:
        resp = requests.get(
            "https://hacker-news.firebaseio.com/v0/topstories.json",
            timeout=10
        )
        resp.raise_for_status()
        story_ids = resp.json()[:limit]
    except Exception as e:
        print(f"[hackernews] Failed to fetch top story IDs: {e}")
        return []
    
    print(f"[hackernews] Fetching details for {len(story_ids)} stories")
    
    # Fetch story details in parallel
    stories = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_id = {
            executor.submit(fetch_hn_story_details, story_id): story_id
            for story_id in story_ids
        }
        
        for future in as_completed(future_to_id):
            story = future.result()
            if story and story.get("type") == "story":
                stories.append(story)
    
    print(f"[hackernews] Successfully fetched {len(stories)} stories")
    return stories


def fetch_and_store_hackernews(conn: Connection, limit: int = 100) -> int:
    """Fetch top HN stories and store in subjects table as 'hackernews' type.
    
    Args:
        conn: Database connection
        limit: Number of top stories to fetch (default 100)
    
    Returns:
        Number of stories successfully stored
    """
    print(f"[hackernews] fetch_and_store_hackernews limit={limit}")
    
    stories = fetch_hn_top_stories(limit)
    
    stored_count = 0
    fetched_at = datetime.now(timezone.utc).isoformat()
    
    for story_data in stories:
        try:
            hn_id = str(story_data["id"])
            
            hn_subject = HackernewsSubject(
                id=hn_id,
                title=story_data.get("title", ""),
                url=story_data.get("url"),
                by=story_data.get("by", ""),
                score=story_data.get("score", 0),
                time=story_data.get("time", 0),
                descendants=story_data.get("descendants", 0),
                extracted_at=fetched_at,
            )
            
            upsert_hackernews_subject(conn, hn_id, hn_subject)
            stored_count += 1
            
        except Exception as e:
            print(f"[hackernews] Failed to store story {story_data.get('id')}: {e}")
            continue
    
    conn.commit()
    print(f"[hackernews] Stored {stored_count} stories in subjects table")
    return stored_count

