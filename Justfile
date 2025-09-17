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

# Reset database (delete all data)
reset-db:
    rm -f app.db
    @echo "Database reset. Run 'just init-db' to recreate tables."

# Check if environment is set up correctly
check:
    @echo "Checking Python version..."
    python --version
    @echo "Checking if .env exists..."
    @test -f .env && echo ".env file exists" || echo "WARNING: .env file missing - copy from .env.example"
    @echo "Checking if database exists..."
    @test -f app.db && echo "Database exists" || echo "WARNING: Run 'just init-db' to create database"
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

# Show available commands
help:
    @just --list
