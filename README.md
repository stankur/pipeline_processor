# Standalone Worker

Self-contained Dagster pipeline with Flask API for processing GitHub users and repositories.

## Setup

```bash
# 1. Install dependencies
just setup

# 2. Configure environment
cp .env.example .env
# Edit .env with your API keys (OPENROUTER_API_KEY, GITHUB_TOKEN)

# 3. Initialize database
just init-db

# 4. Verify setup
just check
```

## Running

### Option 1: API Server (dev)

```bash
# Start the API server
just serve

# Server runs at http://localhost:8080
```

### Option 2: Prod-like local run (gunicorn)

```bash
# Start prod-like server (gunicorn) with optional memory cap
just compose-start            # no cap
just compose-start 256m       # cap memory to 256 MiB

# Watch live resource usage
just compose-stats

# Trigger work
just restart user=<github-username>

# Stop services
just compose-down
```

### Option 3: Dagster UI (for debugging)

```bash
# Start Dagster UI
just dev

# UI available at http://localhost:3000
```

## API Usage

### User login (handles new/ghost/existing users)

```bash
# Unified login endpoint
curl -X POST http://localhost:8080/users/stankur/login

# Returns status: "new" (processing), "activated" (ghost user), or "existing" (ready)
```

### Start pipeline for any user

```bash
# Works with any GitHub username
curl -X POST http://localhost:8080/users/stankur/start
curl -X POST http://localhost:8080/users/octocat/start
curl -X POST http://localhost:8080/users/torvalds/start
```

### Ghost users (pre-populate inactive accounts)

```bash
# Create ghost user and fetch their data once
curl -X POST http://localhost:8080/ghost-users/torvalds

# Ghost users appear in feed with is_ghost=true until they login
# When they login via /login, they're instantly activated (data already ready)
```

### Force restart (clears data and reruns)

```bash
curl -X POST http://localhost:8080/users/stankur/restart
```

### Delete user and all resources

```bash
curl -X DELETE http://localhost:8080/users/stankur
```

### Check progress

```bash
curl http://localhost:8080/users/stankur/progress
```

### Get final data

```bash
curl http://localhost:8080/users/stankur/data
```

### Get daily coding activity

```bash
curl http://localhost:8080/users/stankur/activity
```

Get daily coding activity for a user (repos and languages touched per day).

Returns presence-only data (no commit counts) suitable for building GitHub-style contribution heatmaps with language/repo breakdowns.

**Response Schema:**

```json
{
	"ok": true,
	"activity": {
		"version": 1,
		"window_years": 2,

		// Per-day activity (chronological index)
		"days": {
			"2025-10-26": {
				"repos": ["owner/repo-a", "owner/repo-b"],
				"languages": ["Python", "TypeScript"]
			},
			"2025-10-25": {
				"repos": ["owner/repo-a"],
				"languages": ["Go"]
			}
		},

		// Reverse indexes for fast filtering
		"repo_days": {
			"owner/repo-a": ["2025-10-25", "2025-10-26"],
			"owner/repo-b": ["2025-10-26"]
		},
		"language_days": {
			"Python": ["2025-10-26"],
			"TypeScript": ["2025-10-26"],
			"Go": ["2025-10-25"]
		}
	}
}
```

**Fields:**

-   `version` (int): Schema version for future compatibility
-   `window_years` (int): Time window in years (default: 2)
-   `days` (object): Date-indexed activity. Each date contains:
    -   `repos` (array): Repository IDs touched that day
    -   `languages` (array): Languages used that day
-   `repo_days` (object): Reverse index mapping repo → array of dates
-   `language_days` (object): Reverse index mapping language → array of dates

**Returns `null` if:**

-   User hasn't been processed yet
-   Activity collection task hasn't run

**Use Cases:**

1. **GitHub-style heatmap**: Use `days` to render a calendar grid, color intensity by number of repos/languages
2. **Filter by repo**: Use `repo_days["owner/repo"]` to get active dates for one project
3. **Filter by language**: Use `language_days["Python"]` to show Python-only activity
4. **Toggle view**: Switch between "repos" and "languages" arrays in the same date structure

**Notes:**

-   Only includes commits authored by the user (via GitHub API author filter)
-   Excludes documentation files (.md, .rst, etc.)
-   Excludes vendor/generated code (node_modules, lockfiles, etc.)
-   Languages detected via file extensions, validated against GitHub Linguist data
-   Data refreshes on user login/restart

### For You feed

```bash
# Fast feed from cache (with fatigue ranking)
curl http://localhost:8080/for-you/<viewer_username>

# Rebuild feed (runs LLM judgments, populates cache)
curl -X POST http://localhost:8080/for-you/<viewer_username>
```

-   Returns personalized feed with LLM judgments and fatigue ranking.
-   Candidates from all users' `highlighted_repos`, excluding viewer's own repos.
-   Uses exposure tracking to avoid repetitive items.
-   Each repo includes `username` (author) and `is_ghost` (whether author is inactive).
-   Default limit: 30 repos (add `?limit=N` to customize).

### Repo gallery management

Add or remove images on a repository's gallery (creates the repo subject if missing).

Note: If `API_KEY` is set in the environment, include header `Authorization: Bearer <API_KEY>`.

```bash
# Add a single image (also link this repo to the user)
curl -X POST http://localhost:8080/users/alice/repos/octocat/hello-world/gallery \
  -H 'Content-Type: application/json' \
  -d '{
        "url": "https://example.com/screenshot.png",
        "alt": "Landing page",
        "original_url": "docs/images/shot.png",
        "title": "Build success",
        "caption": "First green pipeline",
        "is_highlight": true,
        "taken_at": 1758888612000,
        "link": true
      }'

# Add multiple images with URL-based dedupe (default). Optional per-item: title, caption, is_highlight, taken_at (epoch ms; defaults to now)
curl -X POST http://localhost:8080/users/alice/repos/octocat/hello-world/gallery \
  -H 'Content-Type: application/json' \
  -d '{
        "images": [
          { "url": "https://example.com/a.png", "alt": "A", "taken_at": 1758794400000 },
          { "url": "https://example.com/b.png", "alt": "B", "is_highlight": true }
        ],
        "dedupe": "url"
      }'

# Delete by URL (query params)
curl -X DELETE 'http://localhost:8080/users/alice/repos/octocat/hello-world/gallery?url=https://example.com/a.png&url=https://example.com/b.png'

# Or delete by body
curl -X DELETE http://localhost:8080/users/alice/repos/octocat/hello-world/gallery \
  -H 'Content-Type: application/json' \
  -d '{ "urls": ["https://example.com/a.png", "https://example.com/b.png"] }'

# Update fields of an existing image by URL (PATCH)
curl -X PATCH http://localhost:8080/users/alice/repos/octocat/hello-world/gallery \
  -H 'Content-Type: application/json' \
  -d '{
        "url": "https://example.com/b.png",
        "is_highlight": false,
        "title": "New title"
      }'
```

## Quick Commands

```bash
# User management
just login alice           # Login endpoint (handles new/ghost/existing)
just ghost alice           # Create ghost user (pre-populate)
just delete alice          # Delete user and all resources

# Pipeline
just start alice           # Start pipeline
just restart alice         # Force restart
just progress alice        # Check progress
just data alice            # Get final data

# Feed
just for-you alice         # Get personalized feed

# Direct pipeline (Dagster)
just run alice             # Run full pipeline directly
just run-selection alice "fetch_repos_asset"  # Run specific assets
```

## Inspection

### Database

```bash
# Start local Supabase (Postgres) for development
just supa-start

# Reset database (delete all data)
just reset-db

# Stop local Supabase
just supa-stop
```

### Database UI (pgweb)

```bash
# Open a lightweight Postgres UI at http://localhost:8081
just db-ui

# Stop the UI container
just db-ui-stop
```

### Logs

-   API logs: printed to console when running `just serve`
-   Dagster logs: in `dagster_logs/` directory
-   Task logs: printed during execution with `[task]` prefix

### Status

```bash
# Environment check
just check

# List available assets
.venv/bin/dagster asset list -m defs
```

## Configuration

### Environment Variables (.env)

-   `DATABASE_URL` - Postgres connection string (defaults to local Supabase at `postgresql://postgres:postgres@localhost:54322/postgres`)
-   `OPENROUTER_API_KEY` - For LLM calls (required for repo selection and blurb generation)
-   `GITHUB_TOKEN` - For GitHub API (optional but recommended for rate limits)
-   `API_KEY` - Optional API key for securing endpoints (include as `Authorization: Bearer <API_KEY>`)
-   `ALLOWED_ORIGINS` - Comma-separated CORS origins (e.g., `http://localhost:3000`)

### Supabase Connection (Production)

For cloud Supabase, set `DATABASE_URL` in your environment:

1. Go to your Supabase project → **Settings** → **Database**
2. Copy **Connection String** → **URI** (Transaction mode)
3. Set as environment variable:
    ```bash
    export DATABASE_URL="postgresql://postgres.[PROJECT-REF]:[PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres"
    ```

### Users

The pipeline works with **any GitHub username** dynamically. No configuration needed - just call the API with any valid GitHub username and it will process that user.

## Troubleshooting

### Common Issues

-   **"Database connection failed"**: Start local Supabase with `just supa-start` or set `DATABASE_URL` for cloud instance
-   **"Permission denied"**: Check virtual environment with `just check`
-   **"API key missing"**: Configure `.env` file
-   **"Git not found"**: Install git (required for repo analysis)

### Reset Everything

```bash
just clean      # Remove Dagster logs/cache
just reset-db   # Reset database (Supabase)
just init-db    # Recreate tables
```
