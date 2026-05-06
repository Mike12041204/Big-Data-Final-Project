from pymongo import MongoClient
from src.cleanLayer import run_clean_layer
from src.rawLayer import run_raw_layer, run_raw_profile
from src.aggregateLayer import run_aggregate_layer
from src.utils.logger import get_logger
from src.utils.reliability import simulate_failure_scenario
from src.config import MONGO_URI

logger = get_logger("Main")

def main():
    logger.info(f"Starting pipeline on URI: {MONGO_URI}")

    try:
        # Demonstrate reliability features (Part 7 requirement)
        logger.info("=== RELIABILITY FEATURES DEMONSTRATION ===")
        simulate_failure_scenario()

        # Step 1: See how messy the data is first
        logger.info("=== STEP 1: RAW DATA PROFILING ===")
        run_raw_profile(sample_size=100000)

        # Step 2: Run the cleaning process with reliability features
        logger.info("=== STEP 2: DATA CLEANING WITH RELIABILITY ===")
        run_clean_layer()

        # Step 3: Run the Business Logic and Graphing with performance analysis
        logger.info("=== STEP 3: AGGREGATION AND VISUALIZATION ===")
        run_aggregate_layer()

        logger.info("=== PIPELINE EXECUTION COMPLETED SUCCESSFULLY ===")
        logger.info("Reliability features demonstrated:")
        logger.info("- Retry logic for MongoDB operations")
        logger.info("- Bad record logging and capture")
        logger.info("- Ingestion checkpointing for resumability")
        logger.info("- Performance analysis with indexing benefits")

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user. Check checkpoints for resumability.")
    except Exception as e:
        logger.error(f"Pipeline crashed: {e}", exc_info=True)
        logger.error("Check bad_records collection for validation failures")
        logger.error("Check ingestion_checkpoints for resumability status")
        raise
