"""Fetch and store GitHub trending repositories."""
from __future__ import annotations
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime, timezone
from psycopg import Connection
from models import RepoSubject
from db import upsert_subject
import json


def scrape_trending_page(language: str | None = None, since: str = "daily") -> List[Dict[str, Any]]:
    """Scrape GitHub trending page for repositories.
    
    Args:
        language: Language filter (e.g., "python", "javascript"), None for all languages
        since: Time period - "daily", "weekly", or "monthly"
    
    Returns:
        List of dicts with keys: owner, name, description, language, stars_today, url
    """
    # Build URL
    url = f"https://github.com/trending"
    if language:
        url += f"/{language}"
    url += f"?since={since}"
    
    print(f"[trending] Scraping {url}")
    
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"[trending] Failed to fetch {url}: {e}")
        return []
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    articles = soup.find_all('article', class_='Box-row')
    
    print(f"[trending] Found {len(articles)} repositories")
    
    repos = []
    for article in articles:
        try:
            # Extract repo owner and name from h2 > a href
            h2 = article.find('h2')
            if not h2:
                continue
            
            link = h2.find('a')
            if not link:
                continue
            
            href = link.get('href', '').strip('/')
            parts = href.split('/')
            if len(parts) < 2:
                continue
            
            owner = parts[0]
            name = parts[1]
            
            # Extract description
            desc_p = article.find('p', class_='col-9')
            description = desc_p.get_text(strip=True) if desc_p else None
            
            # Extract language
            lang_span = article.find('span', attrs={'itemprop': 'programmingLanguage'})
            lang = lang_span.get_text(strip=True) if lang_span else None
            
            # Extract total stars from stargazers link
            stargazers_link = article.find('a', href=lambda h: h and 'stargazers' in h)
            total_stars = 0
            if stargazers_link:
                stars_text = stargazers_link.get_text(strip=True).replace(',', '')
                try:
                    total_stars = int(stars_text)
                except (ValueError, AttributeError):
                    total_stars = 0
            
            # Extract stars today (for logging)
            star_span = article.find('span', class_='d-inline-block float-sm-right')
            stars_today = star_span.get_text(strip=True) if star_span else "0"
            
            repos.append({
                'owner': owner,
                'name': name,
                'description': description,
                'language': lang,
                'total_stars': total_stars,
                'stars_today': stars_today,
                'url': f"https://github.com/{owner}/{name}",
            })
            
        except Exception as e:
            print(f"[trending] Failed to parse article: {e}")
            continue
    
    print(f"[trending] Successfully parsed {len(repos)} repositories")
    return repos


def fetch_and_store_trending_repos(conn: Connection, languages: List[str]) -> int:
    """Fetch trending repos and store in subjects table as 'trending_repo' type.
    
    Args:
        conn: Database connection
        languages: List of language filters (e.g., ["python", "javascript"])
    
    Returns:
        Total number of trending repos stored
    """
    print(f"[trending] fetch_and_store_trending_repos languages={languages}")
    
    all_repos: Dict[str, Dict[str, Any]] = {}  # Keyed by "owner/name" to dedupe
    
    # Scrape all languages (daily, weekly, monthly)
    for since in ["daily", "weekly", "monthly"]:
        repos = scrape_trending_page(language=None, since=since)
        for repo in repos:
            repo_id = f"{repo['owner']}/{repo['name']}"
            if repo_id not in all_repos:
                all_repos[repo_id] = repo
    
    # Scrape each language (daily, weekly, monthly)
    for lang in languages:
        for since in ["daily", "weekly", "monthly"]:
            repos = scrape_trending_page(language=lang, since=since)
            for repo in repos:
                repo_id = f"{repo['owner']}/{repo['name']}"
                if repo_id not in all_repos:
                    all_repos[repo_id] = repo
    
    print(f"[trending] Total unique repos after deduping: {len(all_repos)}")
    
    # Store in subjects table
    stored_count = 0
    scraped_at = datetime.now(timezone.utc).isoformat()
    
    for repo_id, repo_data in all_repos.items():
        try:
            # Create minimal RepoSubject (no galleries, no generated descriptions)
            repo_subject = RepoSubject(
                id=repo_id,
                name=repo_data['name'],
                description=repo_data.get('description'),
                language=repo_data.get('language'),
                stargazers_count=repo_data.get('total_stars', 0),
                fork=False,
                pushed_at=None,  # Not available on trending page
                updated_at=None,  # Not available on trending page
                topics=[],
                extracted_at=scraped_at,  # When WE captured it trending
            )
            
            # Store as 'trending_repo' type
            upsert_subject(conn, 'trending_repo', repo_id, repo_subject.model_dump_json())
            stored_count += 1
            
        except Exception as e:
            print(f"[trending] Failed to store {repo_id}: {e}")
            continue
    
    conn.commit()
    print(f"[trending] Stored {stored_count} trending repos in subjects table")
    return stored_count

