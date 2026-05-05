from src.cleanLayer import run_clean_layer
from src.rawLayer import run_raw_profile 
from src.aggregateLayer import run_aggregate_layer
from src.utils.logger import get_logger
from src.config import MONGO_URI

logger = get_logger("Main")

if __name__ == "__main__":
    logger.info(f"Starting pipeline on URI: {MONGO_URI}")
    try:
        # Step 1: See how messy the data is first
        run_raw_profile(sample_size=100000)
        
        # Step 2: Run the cleaning process
        run_clean_layer()

        # Step 3: Run the Business Logic and Graphing
        run_aggregate_layer() # <-- Run it here
        
        logger.info("Pipeline execution finished successfully.")
    except Exception as e:
        logger.error(f"Pipeline crashed: {e}", exc_info=True)