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
    uv sync
}

# Download data if not already present
if (-not (Test-Path "$PSScriptRoot\nyc_311_raw_data.jsonl")) {
    uv run python src/utilities.py
}

# Run the app (stdout+stderr shown on screen and overwrite log.txt)
uv run python main.py 2>&1 | Tee-Object -FilePath "$PSScriptRoot\log.txt"
popd