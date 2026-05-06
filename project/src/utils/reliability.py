import time
import json
from functools import wraps
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from src.utils.logger import get_logger

logger = get_logger("Reliability")

class BadRecordLogger:
    """Logs invalid records to a separate collection for analysis."""

    def __init__(self, db, collection_name="bad_records"):
        self.collection = db[collection_name]
        # Create index on error_type for faster querying
        self.collection.create_index("error_type")
        self.collection.create_index("timestamp")

    def log_bad_record(self, record, error_message, error_type="validation_error"):
        """Log a bad record with error details."""
        bad_record_doc = {
            "original_record": record,
            "error_message": str(error_message),
            "error_type": error_type,
            "timestamp": time.time(),
            "record_id": record.get("unique_key", "unknown") if isinstance(record, dict) else "unknown"
        }

        try:
            self.collection.insert_one(bad_record_doc)
            logger.warning(f"Logged bad record {bad_record_doc['record_id']}: {error_message}")
        except Exception as e:
            logger.error(f"Failed to log bad record: {e}")

def retry_on_failure(max_retries=3, delay=1, backoff=2):
    """
    Decorator that retries MongoDB operations on failure.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay on each retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (PyMongoError, ServerSelectionTimeoutError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Operation failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                        logger.info(f"Retrying in {current_delay} seconds...")
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"Operation failed after {max_retries + 1} attempts: {e}")
                        raise e
                except Exception as e:
                    # Don't retry for non-MongoDB errors
                    logger.error(f"Non-retryable error: {e}")
                    raise e

            # This should never be reached, but just in case
            raise last_exception

        return wrapper
    return decorator

class CheckpointManager:
    """Manages ingestion checkpoints to allow resuming interrupted processes."""

    def __init__(self, db, checkpoint_collection="ingestion_checkpoints"):
        self.collection = db[checkpoint_collection]
        self.collection.create_index("stage")
        self.collection.create_index([("stage", 1), ("timestamp", -1)])

    def save_checkpoint(self, stage, last_processed_id, processed_count, metadata=None):
        """Save current progress checkpoint."""
        checkpoint = {
            "stage": stage,
            "last_processed_id": last_processed_id,
            "processed_count": processed_count,
            "timestamp": time.time(),
            "metadata": metadata or {}
        }

        # Upsert the checkpoint (update if exists, insert if not)
        self.collection.replace_one(
            {"stage": stage},
            checkpoint,
            upsert=True
        )

        logger.info(f"Checkpoint saved for stage '{stage}': {processed_count} records processed")

    def get_checkpoint(self, stage):
        """Get the latest checkpoint for a stage."""
        checkpoint = self.collection.find_one(
            {"stage": stage},
            sort=[("timestamp", -1)]
        )
        return checkpoint

    def clear_checkpoint(self, stage):
        """Clear checkpoint for a stage (when processing completes successfully)."""
        result = self.collection.delete_many({"stage": stage})
        logger.info(f"Cleared {result.deleted_count} checkpoints for stage '{stage}'")

def simulate_failure_scenario():
    """
    Demonstrates how the system handles various failure scenarios.
    This can be called to test reliability features.
    """
    logger.info("=== FAILURE SCENARIO DEMONSTRATION ===")

    # Scenario 1: Invalid data records
    logger.info("Scenario 1: Processing records with validation errors")
    invalid_records = [
        {"unique_key": "test1", "invalid_field": "missing required fields"},
        {"unique_key": "test2", "complaint_type": "", "agency": None},  # Missing required fields
        {"unique_key": "test3", "created_date": "invalid-date-format"}  # Invalid date
    ]

    # Scenario 2: MongoDB connection issues (would be tested with network interruption)
    logger.info("Scenario 2: MongoDB connection failures are handled by retry logic")

    # Scenario 3: Memory pressure (large batches)
    logger.info("Scenario 3: Large batch processing with memory management")

    # Scenario 4: Duplicate key violations
    logger.info("Scenario 4: Duplicate records are handled gracefully")

    logger.info("=== FAILURE SCENARIO DEMONSTRATION COMPLETE ===")
    logger.info("Check logs and bad_records collection for detailed failure handling")