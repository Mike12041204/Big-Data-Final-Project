# Check if MongoDB is running
$running = docker compose -f docker\compose.yaml ps --services --filter "status=running" | Select-String -Pattern "mongo1"
if (-not $running) {
    # Start MongoDB replica set
    docker compose -f docker\compose.yaml up -d
    # Wait for MongoDB to be ready
    Start-Sleep -Seconds 10
}

# Check if uv environment is set up
pushd project
if (-not (Test-Path ".venv")) {
    # Set up environment with uv
    uv sync
}

# Run the app (stdout+stderr shown on screen and overwrite log.txt)
uv run python main.py 2>&1 | Tee-Object -FilePath "$PSScriptRoot\log.txt"
popd