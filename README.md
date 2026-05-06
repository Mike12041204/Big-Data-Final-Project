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


# 🗽 NYC 311 Big Data Pipeline
**A High-Performance Data Engineering Project**

This project implements a complete Big Data pipeline to process, clean, and analyze over **1.5 million records** of New York City 311 service requests. Utilizing a **3-Node MongoDB Replica Set** for resilient storage and **Polars** for lightning-fast data transformation, this pipeline answers critical business questions regarding urban management and citizen engagement.

---

## 🛠 Tech Stack
* **Database:** MongoDB 7.0 (3-Node Replica Set via Docker)
* **Processing:** Python 3.13 + [Polars](https://pola.rs/) (High-performance DataFrame library)
* **Package Management:** [uv](https://github.com/astral-sh/uv) (Extremely fast Python package installer)
* **Visualization:** Matplotlib & Tableau
* **Infrastructure:** Docker & Docker Compose
* **Reliability:** Retry logic, checkpointing, bad record logging, performance monitoring

---

## 🚀 Getting Started

### 1. Data Acquisition
To replicate this analysis, you must first obtain the raw dataset (`nyc_311_raw_data.jsonl`). 
* **Download:** [Raw Dataset Google Drive Link](https://drive.google.com/drive/u/2/folders/1hF7r125I7OWBUJY7c7dUulJgu3R-rrcA)
* **Alternative:** Run the scripts located in `/DataFetchingScripts`.

### 2. Infrastructure Setup
Spin up the MongoDB Replica Set nodes:
```bash
cd docker
docker compose up -d
```

### 3. Verify the Primary Node
Check the status of the replica set to identify the **Primary** node (this is required for the initial data import):
```bash
docker exec -it docker-mongo1-1 mongosh --eval "rs.status()"
```

### 4. Data Ingestion (Manual Import)
Because of the file size (~1.9 GB), we import the data directly into the container to ensure high-speed loading:

1.  **Copy the file to the Primary container:**
    ```bash
    docker cp path/to/nyc_311_raw_data.jsonl docker-mongo1-1:/raw_311_data.jsonl
    ```
2.  **Import the JSONL into MongoDB:**
    ```bash
    docker exec docker-mongo1-1 mongoimport --db nyc_311_db --collection raw_311 --file /raw_311_data.jsonl
    ```

### 5. Run the Analytics Pipeline
Build and execute the cleaning and aggregation layers:
```bash
# Build and run the app
docker compose up app --build
```

---

## Reliability & performance features

This pipeline implements reliability features to handle real-world Big Data challenges:

### Reliability features (Part 7)
- **Retry logic**: Automatic retry of failed MongoDB operations with exponential backoff
- **Bad record logging**: Invalid records are captured in `bad_records` for analysis
- **Ingestion checkpointing**: Pipeline can resume from interruptions using checkpoint data
- **Failure scenario handling**: Demonstrates graceful handling of several failure types

### Monitoring & observability
- **Logging**: Operations go to `pipeline.log` and the console
- **Progress tracking**: Batch progress during long cleans
- **Error classification**: Different error types are tracked separately

---

## 6. Query modeling and performance

Big Data systems fail when every query scans the full dataset. This project makes two concrete decisions—**compound indexing in MongoDB** and **Hive-style partitioning for Parquet**—and measures them.

### 6.1 Indexing decision (MongoDB)

**What we chose:** In addition to single-field indexes on `borough`, `complaint_type`, and `created_date`, we add a **compound index** `(borough, complaint_type)` named `borough_complaint_compound` on the cleaned collection.

**Why:** Many analytical questions are *slices* of the city: “this borough × this issue type” (for example, noise in Brooklyn vs. heating in the Bronx). A compound index lets the planner seek a narrow key range instead of scanning all documents for one field and filtering the other in memory. The leading key `borough` matches low-cardinality geography; `complaint_type` narrows within each borough.

**Example query that benefits:**

```javascript
db.cleaned_311.find({
  borough: "BROOKLYN",
  complaint_type: "Noise - Street/Sidewalk"
})
```

(The pipeline picks a real `{ borough, complaint_type }` pair that exists in your data so the benchmark is never empty.)

**Before vs after:** At the end of the aggregation step, `src/query_modeling.py` temporarily **drops** only the compound index, runs `explain` with `executionStats`, times a `countDocuments` for that filter, then **recreates** the compound index and repeats. Logs include wall-clock time, `totalDocsExamined`, `executionTimeMillis`, and the winning plan’s index name so you can compare planner behavior—not just wall time.

**Trade-offs:** Extra indexes speed reads but **slow writes** (each insert updates more B-tree pages) and use **disk**. For a read-heavy analytics replica, that is usually acceptable; for a write-heavy primary, we would trim indexes or offload analytics to a secondary.

### 6.2 Partitioning decision (Polars + Parquet)

**What we chose:** We materialize the same in-memory frame twice under `project/outputs/query_modeling_demo/` (ignored by git to avoid huge binaries):

- `monolithic.parquet` — one file for all boroughs  
- `partitioned/borough=<NAME>/part.parquet` — one directory per borough (`hive_partitioning` layout). The borough column is stored only in the **path**, not duplicated inside each file.

**Why:** For filters like `borough == "MANHATTAN"`, a partitioned layout lets the engine **skip entire files** (partition pruning). A single huge file still benefits from columnar reads but must touch more row groups for the same logical slice.

**Example (conceptually):**

```python
pl.scan_parquet("partitioned/**/*.parquet", hive_partitioning=True).filter(
    pl.col("borough") == "MANHATTAN"
)
```

The pipeline logs **monolithic vs partitioned** wall time for the same filter; on large data the gap usually widens.

**Trade-offs:** Many small files can hurt object-store listing performance; **file size** and **partition cardinality** need balance. Here, five boroughs is a safe, interpretable partition key.

### 6.3 Sharding (hypothetical scale-out)

This deployment uses a **replica set**, not a sharded cluster. If the dataset moved to **sharded MongoDB**, a defensible **shard key** would be **`{ borough: 1, unique_key: "hashed" }`** (or `hashed` on `unique_key` with a `borough` prefix). **Rationale:** `borough` avoids a single hot chunk for NYC-wide writes; hashing `unique_key` spreads documents within a borough. **Trade-off:** cross-borough analytics become scatter/gather queries unless we also maintain summary collections (as we do in `analytics_summary`).

### 6.4 How to run the benchmarks

They run automatically after charts are built (`run_aggregate_layer` → `run_performance_analysis`). Watch the log for lines prefixed with `QueryModeling`. Unit tests cover explain parsing:

```bash
cd project
uv run pytest tests/test_query_modeling.py
```

---

## �📊 Results & Artifacts
Once the pipeline finishes (`exited with code 0`), head to the `project/outputs/` directory on your host machine to find the generated insights:

* **`q1_borough_volume.png`**: Total complaints distributed across the five boroughs.
* **`q2_top_complaints.png`**: Visualization of the top 5 most frequent 311 issues.
* **`q3_peak_hours.png`**: Time-series analysis showing peak reporting hours.
* **`q4_channels.png`**: Distribution of preferred reporting methods (Phone, Web, etc.).
* **`tableau_ready_311_data.csv`**: The fully cleaned dataset ready for advanced mapping and dashboarding in Tableau.

---

## 🧪 Development & Testing
Unit tests are used to verify the data cleaning and transformation logic.
```bash
cd project
uv run pytest tests/
```

### Database Access
To manually inspect the data or run queries against the replica set:
```bash
# Shell into the primary node
docker exec -it docker-mongo1-1 mongosh
```

---

> **Note:** This project utilizes `uv` for dependency management. The `pyproject.toml` and `uv.lock` files contain the definitive environment configuration. Please avoid using `requirements.txt` to prevent dependency drift.
