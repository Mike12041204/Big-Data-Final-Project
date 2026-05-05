import json
import os
import time
import urllib.request
import urllib.parse
import logging
import sys
import polars as pl

DATA_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
DATA_OUT = os.path.join(os.path.dirname(__file__), "..", "..", "nyc_311_raw_data.jsonl")

def download_data():
    print("Downloading NYC 311 data...")
    with open(DATA_OUT, "w") as f:
        offset, total = 0, 0
        while offset < 15_000_000:
            params = urllib.parse.urlencode({"$limit": 50000, "$offset": offset, "$order": "created_date DESC"})
            data = json.loads(urllib.request.urlopen(f"{DATA_URL}?{params}", timeout=60).read())
            if not data:
                break
            for r in data:
                f.write(json.dumps(r) + "\n")
            total += len(data)
            offset += 500_000
            time.sleep(0.5)
    print(f"Done. {total:,} records saved to {DATA_OUT}.")

if __name__ == "__main__":
    download_data()

def get_logger(name: str):
    logger = logging.getLogger(name)
    
    # Only add handlers if they don't exist to avoid duplicate logs
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Format: Time - Name - Level - Message
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Output to console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Output to file (Requirement 7: Bad-record logging)
        file_handler = logging.FileHandler("pipeline.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger