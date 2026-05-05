from pymongo import MongoClient
from src.cleanLayer import run_clean_layer
from src.rawLayer import run_raw_layer
from src.aggregateLayer import run_aggregate_layer
from src.utilities import get_logger
from src.config import MONGO_URI, DB_NAME, RAW_COLLECTION, CLEAN_COLLECTION, AGGREGATE_COLLECTION

logger = get_logger("Main")

def main():
    logger.info(f"Starting pipeline on URI: {MONGO_URI}")
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]

        logger.info(f"Clearing existing collections.")
        for collection in [RAW_COLLECTION, CLEAN_COLLECTION, AGGREGATE_COLLECTION]:
            db[collection].delete_many({})

        # Step 2: See how messy the data is first
        run_raw_layer(db)
        
        # Step 3: Run the cleaning process
        run_clean_layer(db)

        # Step 4: Run the Business Logic and Graphing
        run_aggregate_layer(db)
        
        logger.info("Pipeline execution finished successfully.")
    
    except Exception as e:
        logger.error(f"Pipeline crashed: {e}", exc_info=True)

if __name__ == "__main__":
    main()