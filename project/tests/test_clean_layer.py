import polars as pl
from src.models import ComplaintRecord

def test_polars_normalization_logic():
    """
    Test the logic used inside process_and_save_batch.
    Since we want to test the 'cleaning' part, we mimic the DataFrame creation.
    """
    valid_records = [
        {
            "unique_key": "101",
            "borough": "manhattan",  # Needs Uppercase
            "status": "CLOSED",      # Needs Lowercase
            "zip_code": None,        # Needs fill_null
            "latitude": None,
            "longitude": None
        }
    ]
    
    # This mimics the middle of your process_and_save_batch function
    df = pl.DataFrame(valid_records)
    
    df_clean = df.with_columns([
        pl.col("borough").str.to_uppercase(),
        pl.col("status").str.to_lowercase(),
        pl.col("zip_code").fill_null("00000"),
        pl.col("latitude").fill_null(0.0),
        pl.col("longitude").fill_null(0.0)
    ])

    assert df_clean["borough"][0] == "MANHATTAN"
    assert df_clean["status"][0] == "closed"
    assert df_clean["zip_code"][0] == "00000"
    assert df_clean["latitude"][0] == 0.0