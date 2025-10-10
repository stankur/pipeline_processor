#!/usr/bin/env python3
"""
Manual test script for extract_repo_media functionality.

Usage:
    python scripts/test_extract_media.py owner/repo
    
Example:
    python scripts/test_extract_media.py facebook/react
    python scripts/test_extract_media.py vercel/next.js
"""

import sys
import os
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tasks import _extract_repo_media_data


def test_extract_media(repo_id: str):
    """Test media extraction for a specific repository."""
    
    print(f"\n{'='*60}")
    print(f"Testing: {repo_id}")
    print(f"{'='*60}\n")
    
    try:
        owner, name = repo_id.split("/", 1)
    except ValueError:
        print(f"❌ Invalid repo_id format. Expected 'owner/repo', got: {repo_id}")
        return
    
    link, gallery = _extract_repo_media_data(owner, name)
    
    print(f"Link: {link}\n")
    print(f"Gallery ({len(gallery)} images):")
    print(json.dumps(gallery, indent=2))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    repo_id = sys.argv[1]
    test_extract_media(repo_id)
