# Big-Data-Final-Project

A data pipeline project that processes taxi CSV data through raw → clean → aggregate layers using MongoDB.

## Project Structure

- **Raw Layer**: Loads `taxi_trip_data.csv` into MongoDB
- **Clean Layer**: Cleans and validates taxi trip data with Pydantic models
- **Aggregate Layer**: Performs taxi analytics and summary aggregation
- **Performance Layer**: Benchmarks MongoDB query performance with indexing

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

1. **Raw Layer**: Reads `taxi_trip_data.csv` → Inserts into `raw.rawData` collection
2. **Clean Layer**: Processes raw taxi trips → Saves to `raw.cleanData` collection
3. **Aggregate Layer**: Analyzes clean data and saves summary to `aggregated.summary`
4. **Performance Layer**: Benchmarks query time before/after indexes on `passenger_count` and `pickup_location_id`

## Visualization

After running the pipeline, create charts and graphs:

```powershell
# Run visualization script
uv run python visualize.py
```

This generates:
- `trips_by_hour.png` - Trip distribution by hour of day
- `top_pickup_locations.png` - Top 5 pickup locations by trip count
- `payment_types.png` - Payment method distribution
- `query_performance.png` - Index performance improvement
- `compound_index_performance.png` - Compound index benchmark

## Files

- `src/models.py`: Pydantic data models
- `src/rawLayer.py`: Data ingestion
- `src/cleanLayer.py`: Data cleaning & validation
- `src/aggregateLayer.py`: Data aggregation
- `src/project.py`: Main pipeline orchestration
- `docker/`: Docker configuration for MongoDB + app
- `project/`: Python project with dependencies