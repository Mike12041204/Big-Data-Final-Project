# Check if MongoDB is running (standalone instance)
$running = docker ps --filter "name=mongodb" --filter "status=running" | Select-String -Pattern "mongodb"
if (-not $running) {
    Write-Host "MongoDB not running. Starting it..."
    # Start standalone MongoDB
    docker run -d --name mongodb -p 27017:27017 mongo:7.0
    # Wait for MongoDB to be ready
    Start-Sleep -Seconds 10
    Write-Host "MongoDB started successfully!"
}

# Navigate to project directory
pushd project

# Check if uv environment is set up
if (-not (Test-Path ".venv")) {
    Write-Host "Setting up Python environment with uv..."
    uv sync
}

Write-Host "Running the Big Data pipeline..."
>>>>>>> Stashed changes
# Run the app (stdout+stderr shown on screen and overwrite log.txt)
uv run python main.py 2>&1 | Tee-Object -FilePath "$PSScriptRoot\log.txt"

popd

Write-Host "Pipeline execution complete! Check the following:"
Write-Host "- Visualizations: project/outputs/"
Write-Host "- Logs: project/pipeline.log"
Write-Host "- MongoDB data: Run 'docker exec -it mongodb mongosh' to explore"