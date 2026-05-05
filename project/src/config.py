import os

# MongoDB Settings
# Using "mongodb://localhost:27017" if running Python locally
# If running Python inside Docker, use the service name from compose.yaml
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "bigdata_db"
RAW_COLLECTION = "raw_311"
CLEAN_COLLECTION = "cleaned_311"
AGG_COLLECTION = "analytics_summary"

# Project Settings
LOG_LEVEL = "INFO"