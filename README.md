# Big-Data-Final-Project

A data pipeline project that processes CSV data through raw → clean → aggregate layers using MongoDB.

## Project Structure

- **Raw Layer**: Loads CSV data into MongoDB
- **Clean Layer**: Cleans and validates data with Pydantic models
- **Aggregate Layer**: Performs data aggregations and summaries
- **Test Data**: Simple CSV with name/category/value columns

## Setup and Run

### Prerequisites

1. Install Docker Desktop: https://www.docker.com/products/docker-desktop
2. Install UV package manager:
   - Windows: `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`
   - Linux/Mac: `curl -LsSf https://astral.sh/uv/install.sh | sh`

### Running Options

#### Option 1: Fully Containerized (Recommended)
Runs everything in Docker containers - no local setup needed.

```powershell
.\run.ps1
```

#### Option 2: Local Python + Docker MongoDB
Runs Python locally but connects to MongoDB in Docker.

```powershell
.\run-local.ps1
```

#### Option 3: Manual Setup
For development with local MongoDB.

1. Start MongoDB locally (install from https://www.mongodb.com/try/download/community)
2. Set environment: `$env:MONGODB_URI = "mongodb://localhost:27017/"`
3. Run: `cd project; uv run python main.py`

## Data Flow

1. **Raw Layer**: Reads `testData.csv` → Inserts into `raw.rawData` collection
2. **Clean Layer**: Processes raw data → Saves to `raw.cleanData` collection
3. **Aggregate Layer**: Analyzes clean data → Saves summary to `raw.aggregated` collection

## Files

- `src/models.py`: Pydantic data models
- `src/rawLayer.py`: Data ingestion
- `src/cleanLayer.py`: Data cleaning & validation
- `src/aggregateLayer.py`: Data aggregation
- `src/project.py`: Main pipeline orchestration
- `docker/`: Docker configuration for MongoDB + app
- `project/`: Python project with dependencies