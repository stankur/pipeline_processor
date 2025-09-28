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

# Server runs at http://localhost:5000
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

### Start pipeline for any user

```bash
# Works with any GitHub username
curl -X POST http://localhost:5000/users/stankur/start
curl -X POST http://localhost:5000/users/octocat/start
curl -X POST http://localhost:5000/users/torvalds/start
```

### Force restart (clears data and reruns)

```bash
curl -X POST http://localhost:5000/users/stankur/restart
```

### Check progress

```bash
curl http://localhost:5000/users/stankur/progress
```

### Get final data

```bash
curl http://localhost:5000/users/stankur/data
```

## Direct Pipeline Commands

```bash
# Run full pipeline for any user
just run stankur
just run octocat
just run torvalds

# Run specific assets for any user
just run-selection stankur "fetch_repos_asset,select_highlighted_repos_asset"
```

## Inspection

### Database

```bash
# Check if database exists
ls -la app.db

# Reset database (delete all data)
just reset-db
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

-   `NET_DB_PATH` - Path to SQLite database (default: `./app.db`)
-   `OPENROUTER_API_KEY` - For LLM calls (required for repo selection and blurb generation)
-   `GITHUB_TOKEN` - For GitHub API (optional but recommended for rate limits)

### Users

The pipeline works with **any GitHub username** dynamically. No configuration needed - just call the API with any valid GitHub username and it will process that user.

## Troubleshooting

### Common Issues

-   **"Database not found"**: Run `just init-db`
-   **"Permission denied"**: Check virtual environment with `just check`
-   **"API key missing"**: Configure `.env` file
-   **"Git not found"**: Install git (required for repo analysis)

### Reset Everything

```bash
just clean      # Remove Dagster logs/cache
just reset-db   # Delete database
just init-db    # Recreate database
```
