# Check if MongoDB is running
$running = docker compose -f docker\compose.yaml ps --services --filter "status=running" | Select-String -Pattern "mongo1"
if (-not $running) {
    # Start MongoDB replica set
    docker compose -f docker\compose.yaml up -d
    # Wait for MongoDB to be ready
    Start-Sleep -Seconds 10
}

# Check if uv environment is set up
cd project
if (-not (Test-Path ".venv")) {
    # Set up environment with uv
    uv sync
}

# Run the app
python main.py