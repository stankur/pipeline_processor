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

### Option 1: API Server (recommended)

```bash
# Start the API server
just serve

# Server runs at http://localhost:5000
```

### Option 2: Dagster UI (for debugging)

```bash
# Start Dagster UI
just dev

# UI available at http://localhost:3000
```

## API Usage

### Start pipeline for a user

```bash
curl -X POST http://localhost:5000/users/stankur/start
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
# Run full pipeline for a user
just run stankur

# Run specific assets
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

Edit `assets.py` to add/remove users in the `StaticPartitionsDefinition`:

```python
users_partitions = StaticPartitionsDefinition(["stankur", "ProjectsByJackHe", "SamuelmdLow"])
```

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
