<<<<<<< HEAD
# Run locally but connect to Docker MongoDB
# Check if Docker MongoDB is running
$running = docker compose -f docker\compose.yaml ps --services --filter "status=running" | Select-String -Pattern "mongo1"
if (-not $running) {
    Write-Host "Starting Docker MongoDB..."
    docker compose -f docker\compose.yaml up -d
    Start-Sleep -Seconds 10
}

# Run the app locally with explicit MongoDB URI
pushd project
if (-not (Test-Path ".venv")) {
    Write-Host "Setting up Python environment..."
    uv sync
}

Write-Host "Running application with Docker MongoDB..."
$env:MONGODB_URI = "mongodb://localhost:27017/?directConnection=true"
uv run python main.py

Write-Host "Pipeline complete. Run visualizations with: uv run python visualize.py"
=======
# Run locally but connect to Docker MongoDB
# Check if Docker MongoDB is running
$running = docker compose -f docker\compose.yaml ps --services --filter "status=running" | Select-String -Pattern "mongo1"
if (-not $running) {
    Write-Host "Starting Docker MongoDB..."
    docker compose -f docker\compose.yaml up -d
    Start-Sleep -Seconds 10
}

# Run the app locally with explicit MongoDB URI
pushd project
if (-not (Test-Path ".venv")) {
    Write-Host "Setting up Python environment..."
    uv sync
}

Write-Host "Running application with Docker MongoDB..."
$env:MONGODB_URI = "mongodb://localhost:27017/?directConnection=true"
uv run python main.py

Write-Host "Pipeline complete. Run visualizations with: uv run python visualize.py"
>>>>>>> 31a89514fe5c607ba63bb0574cb71393aba7e6a1
popd