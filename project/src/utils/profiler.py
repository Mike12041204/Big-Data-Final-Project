import polars as pl
from src.utils.logger import get_logger

# Shows all features in table
pl.Config.set_tbl_rows(100) 
pl.Config.set_tbl_cols(20)

logger = get_logger("Profiler")

def profile_layer(df: pl.DataFrame, layer_name: str):
    """
    Performs a health check on the dataframe.
    Calculates Null counts, Zero counts, and percentages.
    """
    total_rows = len(df)
    logger.info(f"--- Profiling Report: {layer_name} ({total_rows} records) ---")

    # We create a list of metrics for each column
    stats = []
    for col in df.columns:
        null_count = df[col].null_count()
        # Check for zeros (only for numeric columns to avoid errors)
        zero_count = 0
        if df[col].dtype in [pl.Float64, pl.Int64]:
            zero_count = (df[col] == 0).sum()

        stats.append({
            "column": col,
            "type": str(df[col].dtype),
            "nulls": null_count,
            "null_pct": f"{(null_count / total_rows) * 100:.2f}%",
            "zeros": zero_count,
            "zero_pct": f"{(zero_count / total_rows) * 100:.2f}%"
        })

    # Print as a nice table
    stats_df = pl.DataFrame(stats)
    print(stats_df)
    return stats_df