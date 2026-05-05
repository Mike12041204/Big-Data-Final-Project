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

## 📊 Results & Artifacts
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
