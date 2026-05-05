import polars as pl
from src.config import RAW_COLLECTION, BATCH_SIZE
from src.utilities import get_logger, DATA_OUT

logger = get_logger("RawLayer")

def run_raw_layer(db):
    logger.info("Loading raw JSONL data into DataFrame.")

    # .jsonl into df
    df = pl.read_ndjson(DATA_OUT)

    # load df into collection
    collection = db[RAW_COLLECTION]
    records = df.to_dicts()
    
    for i in range(0, len(records), BATCH_SIZE):
        collection.insert_many(records[i:i + BATCH_SIZE])

    logger.info(f"Inserted records into '{RAW_COLLECTION}'.")