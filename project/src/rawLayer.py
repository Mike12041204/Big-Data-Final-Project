import os

import polars as pl
from pymongo import MongoClient

from src.config import MONGO_URI, DB_NAME, RAW_COLLECTION, BATCH_SIZE
from src.utilities import get_logger, DATA_OUT

logger = get_logger("RawLayer")


def run_raw_profile(sample_size: int = 100_000) -> None:
    """
    Summarize raw 311 data before cleaning: sample from MongoDB, or first N rows of JSONL if DB is empty.
    """
    logger.info("Profiling raw data (sample up to %s rows)...", sample_size)
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][RAW_COLLECTION]
    total = col.count_documents({})

    if total > 0:
        take = min(sample_size, total)
        docs = list(col.find({}).limit(take))
        for d in docs:
            d.pop("_id", None)
        logger.info("MongoDB %s: %s total docs, profiling %s", RAW_COLLECTION, total, take)
    elif os.path.isfile(DATA_OUT):
        logger.warning(
            "MongoDB %s is empty; profiling first rows from %s instead.",
            RAW_COLLECTION,
            DATA_OUT,
        )
        df_file = pl.read_ndjson(DATA_OUT, n_rows=min(sample_size, 500_000))
        _log_raw_frame_stats(df_file)
        return
    else:
        logger.warning(
            "No raw data (empty %s and missing file %s). Skipping raw profile.",
            RAW_COLLECTION,
            DATA_OUT,
        )
        return

    if not docs:
        return

    df = pl.from_dicts(docs, infer_schema_length=min(len(docs), 10_000))
    _log_raw_frame_stats(df)


def _log_raw_frame_stats(df: pl.DataFrame) -> None:
    logger.info("Raw sample shape: %s rows x %s columns", df.height, df.width)
    logger.info("Schema: %s", {name: str(dtype) for name, dtype in df.schema.items()})
    nc_row = df.null_count().row(0)
    ranked = sorted(zip(df.columns, nc_row), key=lambda x: -int(x[1]))[:15]
    logger.info("Top columns by null count (column, nulls): %s", ranked)


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