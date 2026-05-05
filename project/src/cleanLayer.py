import gc
import polars as pl
from src.models import ComplaintRecord
from src.utilities import get_logger
from src.config import DB_NAME, RAW_COLLECTION, CLEAN_COLLECTION, BATCH_SIZE

logger = get_logger("CleanLayer")

def run_clean_layer(db):

    raw_col = db[RAW_COLLECTION]
    clean_col = db[CLEAN_COLLECTION]

    # Sanity Check: Ensure we actually see the 1.5M records
    total_docs = raw_col.count_documents({})
    logger.info(f"Connected to {DB_NAME}. Found {total_docs} records to process.")

    if total_docs == 0:
        logger.error("No data found in Raw Collection. Aborting.")
        return

    # Clear old data and prepare unique index
    clean_col.delete_many({})
    clean_col.create_index("unique_key", unique=True)

    # .batch_size(2000) on the cursor prevents the DB driver from 
    # pre-fetching too many documents into RAM at once.
    cursor = raw_col.find({}).batch_size(2000)
    
    batch = []
    total_processed = 0

    for record in cursor:
        batch.append(record)
        
        if len(batch) >= BATCH_SIZE:
            process_and_save_batch(batch, clean_col)
            total_processed += len(batch)
            logger.info(f"Progress: {total_processed}/{total_docs} records...")
            
            # --- MEMORY RECLAMATION ---
            batch.clear()  # Empty the list
            gc.collect()   # Force the Garbage Collector to run
            # --------------------------

    # Handle the final remaining records
    if batch:
        process_and_save_batch(batch, clean_col)
        logger.info(f"ETL Pipeline Complete. Total Cleaned: {total_processed + len(batch)}")

def process_and_save_batch(batch, collection):
    """Validates data with Pydantic and cleans with Polars."""
    valid_records = []
    
    for r in batch:
        try:
            # Pydantic handles the snake_case aliases and date/float parsing
            validated = ComplaintRecord(**r)
            valid_records.append(validated.model_dump(by_alias=False))
        except Exception:
            # Silently skip records that don't meet the schema
            continue 

    if not valid_records:
        return

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
    except Exception:
        pass

    # Final cleanup for this batch
    del clean_dicts
    del df