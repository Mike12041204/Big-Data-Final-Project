#!/bin/bash

# Check if MongoDB is running
if ! docker compose -f docker/compose.yaml ps --services --filter "status=running" | grep -q "mongo1"; then
    # Start MongoDB replica set
    docker compose -f docker/compose.yaml up -d
    # Wait for MongoDB to be ready
    sleep 10
fi

# Check if uv environment is set up
pushd project
if [ ! -d ".venv" ]; then
    # Set up environment with uv
    uv sync
fi

# Run the app
uv run python main.py
popd