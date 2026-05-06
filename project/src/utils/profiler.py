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

    # Print as a nice table (encoding-safe)
    stats_df = pl.DataFrame(stats)
    print(f"\n--- Profiling Report: {layer_name} ({total_rows} records) ---")
    print("Column Name".ljust(30) + "Type".ljust(15) + "Nulls".ljust(10) + "Null%".ljust(10) + "Zeros".ljust(10) + "Zero%")
    print("-" * 85)
    for row in stats_df.rows():
        col_name = str(row[0])[:29]  # Truncate long column names
        col_type = str(row[1])[:14]
        nulls = str(row[2])
        null_pct = str(row[3])
        zeros = str(row[4])
        zero_pct = str(row[5])
        print(f"{col_name:<30}{col_type:<15}{nulls:<10}{null_pct:<10}{zeros:<10}{zero_pct}")
    print("-" * 85)
    return stats_df