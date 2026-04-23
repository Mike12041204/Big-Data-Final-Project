# Big-Data-Final-Project

## Setup and Run

To set up the environment and run the program with a single command:

1. Install Docker:
   - Windows: install Docker Desktop from https://www.docker.com/products/docker-desktop
   - Linux/Mac: install Docker Engine or Docker Desktop following https://docs.docker.com/get-docker/
2. Install UV:
   - Windows: `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`
   - Linux/Mac: `curl -LsSf https://astral.sh/uv/install.sh | sh`
3. Run script:
   - Windows: `.\run.ps1`
   - Linux/Mac: `./run.sh`

This will start the MongoDB replica set in Docker, install dependencies with uv, and run the application.