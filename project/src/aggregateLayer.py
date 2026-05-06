import os
import time
import polars as pl
import matplotlib.pyplot as plt
from src.config import MONGO_URI, DB_NAME, CLEAN_COLLECTION, AGG_COLLECTION
from src.utils.logger import get_logger
from src.utils.performance import run_performance_analysis
from src.utils.reliability import retry_on_failure

logger = get_logger("AggregateLayer")

@retry_on_failure(max_retries=3)
def store_aggregation_result(collection, aggregation_name, data, description):
    """Store aggregated results in MongoDB with retry logic."""
    doc = {
        "aggregation_name": aggregation_name,
        "data": data,
        "description": description,
        "timestamp": time.time(),
        "record_count": len(data)
    }

    collection.insert_one(doc)
    logger.info(f"Stored aggregation '{aggregation_name}' with {len(data)} records in MongoDB")

def run_aggregate_layer():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
>>>>>>> Stashed changes
    clean_col = db[CLEAN_COLLECTION]
    agg_col = db[AGG_COLLECTION]

    logger.info("Fetching clean data for Aggregation")

    # Grab the cursor but don't pull everything into a list yet!
    cursor = clean_col.find({}, {"_id": 0}).batch_size(20000)

    df_list = []
    batch = []

    # Process 50,000 rows at a time
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= 50000:
            df_list.append(pl.DataFrame(batch))
            batch.clear() # Empty the Python list to free up RAM!

    # Catch any leftover records from the final batch
    if batch:
        df_list.append(pl.DataFrame(batch))

    # If the database was empty, stop here
    if not df_list:
        logger.error("No clean data found to aggregate!")
        return

    # Stitch all the small DataFrames into one massive, highly-efficient DataFrame
    df = pl.concat(df_list)

    logger.info(f"Loaded {len(df)} records into Polars. Beginning analysis...")

    # Create an outputs folder for the graphs
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)

    # Clear old aggregated data
    agg_col.delete_many({})

    # ---------------------------------------------------------

    # ---------------------------------------------------------
    # Q1: Volume Analysis (Complaints by Borough)
    # ---------------------------------------------------------
    logger.info("Generating Q1: Borough Volume...")
    q1 = df.group_by("borough").agg(pl.len().alias("count")).sort("count", descending=True)

    # Store aggregated results in MongoDB
    q1_results = q1.to_dicts()
    store_aggregation_result(agg_col, "borough_volume", q1_results,
                           "Total complaints by borough - shows which areas have highest service request volume")

    plt.figure(figsize=(10, 6))
    plt.bar(q1["borough"], q1["count"], color='skyblue')
    plt.title("Total 311 Complaints by Borough")
    plt.xlabel("Borough")
    plt.ylabel("Number of Complaints")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/q1_borough_volume.png")
    plt.close()

    # ---------------------------------------------------------
    # Q2: Trend Analysis (Top 5 Complaint Types)
    # ---------------------------------------------------------
    logger.info("Generating Q2: Top 5 Complaint Types...")
    q2 = df.group_by("complaint_type").agg(pl.len().alias("count")).sort("count", descending=True).head(5)

    # Store aggregated results in MongoDB
    q2_results = q2.to_dicts()
    store_aggregation_result(agg_col, "top_complaint_types", q2_results,
                           "Top 5 most frequent complaint types - identifies priority service areas")

    plt.figure(figsize=(10, 6))
    # Horizontal bar chart is better for reading long complaint names
    plt.barh(q2["complaint_type"], q2["count"], color='coral')
    plt.gca().invert_yaxis() # Puts the highest value at the top
    plt.title("Top 5 Most Frequent 311 Complaints")
    plt.xlabel("Number of Complaints")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/q2_top_complaints.png")
    plt.close()

    # ---------------------------------------------------------
    # Q3: Time-Series Analysis (Peak Hours)
    # ---------------------------------------------------------
    logger.info("Generating Q3: Peak Hours...")
    # Extract the hour from the datetime column
    q3 = df.with_columns(pl.col("created_date").dt.hour().alias("hour")) \
           .group_by("hour").agg(pl.len().alias("count")).sort("hour")

    # Store aggregated results in MongoDB
    q3_results = q3.to_dicts()
    store_aggregation_result(agg_col, "complaints_by_hour", q3_results,
                           "Complaint volume by hour of day - shows peak reporting times for resource planning")

    plt.figure(figsize=(10, 6))
    plt.plot(q3["hour"], q3["count"], marker='o', linestyle='-', color='purple')
    plt.title("311 Complaint Volume by Hour of Day")
    plt.xlabel("Hour of Day (0-23)")
    plt.ylabel("Number of Complaints")
    plt.grid(True, alpha=0.3)
    plt.xticks(range(0, 24))
    plt.tight_layout()
    plt.savefig(f"{output_dir}/q3_peak_hours.png")
    plt.close()

    # ---------------------------------------------------------
    # Q4: Channel Optimization (Reporting Methods)
    # ---------------------------------------------------------
    logger.info("Generating Q4: Reporting Channels...")
    q4 = df.group_by("channel_type").agg(pl.len().alias("count")).sort("count", descending=True)

    # Store aggregated results in MongoDB
    q4_results = q4.to_dicts()
    store_aggregation_result(agg_col, "reporting_channels", q4_results,
                           "Distribution of reporting methods - shows preferred channels for citizen engagement")

    plt.figure(figsize=(8, 8))
    plt.pie(q4["count"], labels=q4["channel_type"], autopct='%1.1f%%', startangle=140)
    plt.title("Preferred Reporting Channels for 311")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/q4_channels.png")
    plt.close()

    # ---------------------------------------------------------
    # PERFORMANCE ANALYSIS (Part 6 Requirement)
    # ---------------------------------------------------------
    logger.info("Running performance analysis...")
    run_performance_analysis()

    logger.info(f"Aggregation complete! All graphs saved to the '{output_dir}/' folder.")
    logger.info("Aggregated results stored in MongoDB analytics_summary collection.")