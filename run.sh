#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Check if MongoDB is running
if ! docker compose -f docker/compose.yaml ps --services --filter "status=running" | grep -q "mongo1"; then
    docker compose -f docker/compose.yaml up -d
    sleep 10
fi

cd "$SCRIPT_DIR/project"

# Check if uv environment is set up
if [ ! -d ".venv" ]; then
    uv sync
fi

# Download data if not already present
if [ ! -f "$SCRIPT_DIR/nyc_311_raw_data.jsonl" ]; then
    uv run python src/scripts.py
fi

# Run the app
uv run python main.py 2>&1 | tee "$SCRIPT_DIR/log.txt"
