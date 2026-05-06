import gc
import time
import polars as pl
from src.models import ComplaintRecord
from src.utils.logger import get_logger
from src.utils.profiler import profile_layer
from src.utils.reliability import BadRecordLogger, retry_on_failure, CheckpointManager
from src.config import MONGO_URI, DB_NAME, RAW_COLLECTION, CLEAN_COLLECTION

def run_clean_layer(batch_size=15000):
    """
    Main orchestrator for the cleaning process with reliability features.
    Batch size lowered to 15k to stay safely under 2GB RAM limits.
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
>>>>>>> Stashed changes
    raw_col = db[RAW_COLLECTION]
    clean_col = db[CLEAN_COLLECTION]

    # Initialize reliability components
    bad_record_logger = BadRecordLogger(db)
    checkpoint_mgr = CheckpointManager(db)

    # Check for existing checkpoint to resume processing
    checkpoint = checkpoint_mgr.get_checkpoint("clean_layer")
    start_offset = checkpoint.get("processed_count", 0) if checkpoint else 0

    logger.info(f"Connected to {DB_NAME}. Starting from offset: {start_offset}")

    # Sanity Check: Ensure we actually see the 1.5M records
    total_docs = raw_col.count_documents({})
    logger.info(f"Found {total_docs} total records to process.")

    if total_docs == 0:
        logger.error("No data found in Raw Collection. Aborting.")
        return

    # Clear old data only if starting fresh (no checkpoint)
    if start_offset == 0:
        clean_col.delete_many({})
        clean_col.create_index("unique_key", unique=True)
        # Additional indexes for performance (Part 6 requirement)
        clean_col.create_index("borough")  # For aggregation queries
        clean_col.create_index("complaint_type")  # For filtering queries
        clean_col.create_index([("created_date", 1)])  # For time-based queries

    # .batch_size(2000) on the cursor prevents the DB driver from
    # pre-fetching too many documents into RAM at once.
    cursor = raw_col.find({}).batch_size(2000)

    # Skip already processed records if resuming
    if start_offset > 0:
        cursor.skip(start_offset)
        logger.info(f"Resuming from record {start_offset}")

    batch = []
    total_processed = start_offset
    bad_records_count = 0

    start_time = time.time()

    for record in cursor:
        batch.append(record)
        if len(batch) >= batch_size:
            batch_result = process_and_save_batch_with_reliability(
                batch, clean_col, bad_record_logger
            )
            total_processed += batch_result["processed"]
            bad_records_count += batch_result["bad_records"]

            logger.info(f"Progress: {total_processed}/{total_docs} records processed "
                       f"({bad_records_count} bad records)")

            # Save checkpoint every batch
            checkpoint_mgr.save_checkpoint(
                "clean_layer",
                record.get("unique_key", total_processed),
                total_processed,
                {"bad_records": bad_records_count}
            )
            # --- MEMORY RECLAMATION ---
            batch.clear()  # Empty the list
            gc.collect()   # Force the Garbage Collector to run
            # --------------------------

    # Handle the final remaining records
    if batch:
        batch_result = process_and_save_batch_with_reliability(
            batch, clean_col, bad_record_logger
        )
        total_processed += batch_result["processed"]
        bad_records_count += batch_result["bad_records"]

    # Clear checkpoint on successful completion
    checkpoint_mgr.clear_checkpoint("clean_layer")

    end_time = time.time()
    processing_time = end_time - start_time

    logger.info(f"ETL Pipeline Complete. Total Cleaned: {total_processed}, "
               f"Bad Records: {bad_records_count}, Time: {processing_time:.2f}s")

    # --- RUN THE HEALTH CHECK ---
    logger.info("Running post-clean health check...")
    clean_sample = list(clean_col.find().limit(100000))
    if clean_sample:
        clean_df = pl.DataFrame(clean_sample)
        profile_layer(clean_df, "CLEAN_DATA_POST_CLEAN")

@retry_on_failure(max_retries=3, delay=1)
def process_and_save_batch_with_reliability(batch, collection, bad_record_logger):
    """
    Validates data with Pydantic and cleans with Polars.
    Returns dict with processed count and bad records count.
    Includes retry logic and bad record logging.
    """
    valid_records = []
    bad_records_count = 0

    for r in batch:
        try:
            # Pydantic handles the snake_case aliases and date/float parsing
            validated = ComplaintRecord(**r)
            valid_records.append(validated.model_dump(by_alias=False))
        except Exception as e:
            # Log bad records instead of silently skipping
            bad_record_logger.log_bad_record(r, str(e), "validation_error")
            bad_records_count += 1
            continue

    result = {"processed": len(valid_records), "bad_records": bad_records_count}

    if not valid_records:
        return result

    # Convert to Polars for normalization
    df = pl.DataFrame(valid_records)

    # Explicitly clear the list of dicts now that we have a DataFrame
    del valid_records

    # Normalization logic
    df = df.with_columns([
        pl.col("borough").str.to_uppercase(),
        pl.col("status").str.to_lowercase(),
        pl.col("zip_code").fill_null("00000"),
        pl.col("latitude").fill_null(0.0),
        pl.col("longitude").fill_null(0.0)
    ]).unique(subset=["unique_key"])

    # Prepare for insert
    clean_dicts = df.to_dicts()

    try:
        # ordered=False allows the batch to continue even if one row is a duplicate
        collection.insert_many(clean_dicts, ordered=False)
    except Exception as e:
        logger.error(f"Failed to insert batch after retries: {e}")
        # Log the entire batch as bad records if insert fails
        for record in clean_dicts:
            bad_record_logger.log_bad_record(record, f"Insert failed: {e}", "insert_error")
        result["bad_records"] += len(clean_dicts)
        result["processed"] = 0

    # Final cleanup for this batch
    del clean_dicts
    del df

    return result