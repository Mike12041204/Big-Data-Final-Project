from pymongo import MongoClient
import polars as pl
from src.config import MONGO_URI, DB_NAME, RAW_COLLECTION
from src.utils.profiler import profile_layer
from src.utils.logger import get_logger

logger = get_logger("RawLayer")

def run_raw_profile(sample_size=100000):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    logger.info(f"Sampling {sample_size} records from {RAW_COLLECTION} for profiling...")
    raw_data = list(db[RAW_COLLECTION].find().limit(sample_size))
    
    if not raw_data:
        logger.error("No raw data found to profile!")
        return

    # FIX: Add strict=False to allow messy, mixed-type columns
    df = pl.DataFrame(raw_data, strict=False)
    profile_layer(df, "RAW_DATA_PRE_CLEAN")