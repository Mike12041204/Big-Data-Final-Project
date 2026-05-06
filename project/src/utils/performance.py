import polars as pl
from pymongo.database import Database

from src.query_modeling import run_all
from src.utils.logger import get_logger

logger = get_logger("Performance")


def run_performance_analysis(db: Database, df: pl.DataFrame, output_dir: str = "outputs") -> None:
    """Part 6: indexing, partitioning, explain output, and timing (see README §6)."""
    logger.info("=== Part 6: Query modeling and performance ===")
    run_all(db, df, output_dir)
