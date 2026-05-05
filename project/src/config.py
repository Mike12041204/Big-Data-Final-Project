import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "db"
RAW_COLLECTION = "rawData"
CLEAN_COLLECTION = "cleanData"
AGGREGATE_COLLECTION = "aggregateData"

BATCH_SIZE = 100000