# Worker Justfile - Common commands for the standalone Dagster worker

# Set up the virtual environment and install dependencies
setup:
    python -m venv .venv
    .venv/bin/pip install --upgrade pip
    .venv/bin/pip install -r requirements.txt
    @echo "Setup complete! Don't forget to copy .env.example to .env and configure it."
    @echo "Then run 'just init-db' to create the database."

# Activate virtual environment (run with: source $(just activate))
activate:
    @echo ".venv/bin/activate"

# Run Dagster dev server (with UI)
dev:
    .venv/bin/dagster dev -m defs

# Run Flask API server
serve:
    .venv/bin/python api.py

# Run pipeline for a specific user
run user="stankur":
    .venv/bin/dagster asset materialize -m defs --partition-key {{user}}

# Run specific assets for a user
run-selection user="stankur" selection="fetch_repos_asset,select_highlighted_repos_asset":
    .venv/bin/dagster asset materialize -m defs --partition-key {{user}} --selection {{selection}}

# Initialize the database (create tables)
init-db:
    .venv/bin/python -c "from db import init_db; init_db(); print('Database initialized successfully')"

# Supabase local (minimal)
supa-init:
    npx --yes supabase init

supa-start:
    npx --yes supabase start

supa-stop:
    npx --yes supabase stop


# Reset database (Supabase Postgres)
reset-db:
    npx --yes supabase db reset --no-verify

# Check if environment is set up correctly
check:
    @echo "Checking Python version..."
    python --version
    @echo "Checking if .env exists..."
    @test -f .env && echo ".env file exists" || echo "WARNING: .env file missing - copy from .env.example"
    @echo "Checking if git is available..."
    git --version
    @echo "Checking virtual environment..."
    @test -d .venv && echo "Virtual environment exists" || echo "WARNING: Run 'just setup' first"

# Clean up generated files
clean:
    rm -rf .dagster/
    rm -rf dagster_logs/
    rm -rf __pycache__/
    rm -rf .ruff_cache/
    rm -rf .mypy_cache/
    rm -rf .pytest_cache/

# API wrapper commands
start user="stankur":
    @[ -n "$API_KEY" ] && curl -X POST -H "Authorization: Bearer $API_KEY" http://localhost:8080/users/{{user}}/start || curl -X POST http://localhost:8080/users/{{user}}/start

restart user="stankur":
    @[ -n "$API_KEY" ] && curl -X POST -H "Authorization: Bearer $API_KEY" http://localhost:8080/users/{{user}}/restart || curl -X POST http://localhost:8080/users/{{user}}/restart

delete user="stankur":
    @[ -n "$API_KEY" ] && curl -X DELETE -H "Authorization: Bearer $API_KEY" http://localhost:8080/users/{{user}} || curl -X DELETE http://localhost:8080/users/{{user}}

ghost user="stankur":
    @[ -n "$API_KEY" ] && curl -X POST -H "Authorization: Bearer $API_KEY" http://localhost:8080/ghost-users/{{user}} || curl -X POST http://localhost:8080/ghost-users/{{user}}

login user="stankur":
    @[ -n "$API_KEY" ] && curl -X POST -H "Authorization: Bearer $API_KEY" http://localhost:8080/users/{{user}}/login || curl -X POST http://localhost:8080/users/{{user}}/login

progress user="stankur":
    @[ -n "$API_KEY" ] && curl -H "Authorization: Bearer $API_KEY" http://localhost:8080/users/{{user}}/progress || curl http://localhost:8080/users/{{user}}/progress

data user="stankur":
    @[ -n "$API_KEY" ] && curl -H "Authorization: Bearer $API_KEY" http://localhost:8080/users/{{user}}/data || curl http://localhost:8080/users/{{user}}/data

# For You feed - fast (from cached recommendations)
for-you user="stankur" limit="30":
    @[ -n "$API_KEY" ] && curl -H "Authorization: Bearer $API_KEY" "http://localhost:8080/for-you/{{user}}?limit={{limit}}" | jq || curl "http://localhost:8080/for-you/{{user}}?limit={{limit}}" | jq

# Rebuild for-you feed - slow (runs LLM judgments, populates cache)
for-you-build user="stankur" limit="30":
    @[ -n "$API_KEY" ] && curl -X POST -H "Authorization: Bearer $API_KEY" "http://localhost:8080/for-you/{{user}}?limit={{limit}}" | jq || curl -X POST "http://localhost:8080/for-you/{{user}}?limit={{limit}}" | jq

# Restart from a specific asset key (and downstream)
restart-from user="stankur" start="generate_repo_blurb_asset":
    @[ -n "$API_KEY" ] && curl -X POST -H "Authorization: Bearer $API_KEY" "http://localhost:8080/users/{{user}}/restart-from?start={{start}}" || curl -X POST "http://localhost:8080/users/{{user}}/restart-from?start={{start}}"

# Show available commands
help:
    @just --list

# Docker Compose wrappers (avoid name clash with existing `start` recipe)
compose-start mem="":
    docker compose up --build -d
    @val="{{mem}}"; if [ -n "$val" ]; then \
      echo "$val" | grep -Eq '^[0-9]+[mMgG]$' || { echo "Invalid mem value: '$val'. Use e.g. 256m or 1g."; exit 2; }; \
      cid=$(docker compose ps -q api); \
      if [ -z "$cid" ]; then echo "api not running"; exit 1; fi; \
      docker update --memory "$val" --memory-swap "$val" "$cid"; \
      echo "Applied memory cap $val to $cid"; \
    fi

compose-down:
	docker compose down

compose-stats:
	@cid=$(docker compose ps -q api); if [ -z "$cid" ]; then echo "api not running"; exit 1; fi; docker stats "$cid"

# Lightweight Postgres UI (pgweb) at http://localhost:8081
db-ui:
    @docker rm -f pgweb 2>/dev/null || true
    docker run --rm --name pgweb -p 8081:8081 \
      --add-host=host.docker.internal:host-gateway \
      sosedoff/pgweb \
      --url postgresql://postgres:postgres@host.docker.internal:54322/postgres?sslmode=disable

db-ui-stop:
    docker rm -f pgweb 2>/dev/null || true

# Type checking with BasedPyright
typecheck:
    npx --yes basedpyright

typecheck-errors:
    npx --yes basedpyright --level error
