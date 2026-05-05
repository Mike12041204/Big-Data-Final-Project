import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
from src.rawLayer import rawLayer
from src.cleanLayer import cleanLayer
from src.aggregateLayer import aggregateLayer
from src.performanceLayer import performanceLayer

def project():
    mongodb_uri = os.getenv("MONGODB_URI")

    if not mongodb_uri:
        raise Exception("MONGODB_URI not set")

    print(f" - Connecting to MongoDB at {mongodb_uri}")

    try:
        client = MongoClient(
            mongodb_uri,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=30000,
            socketTimeoutMS=60000,
            maxPoolSize=20,
            minPoolSize=2,
            retryWrites=True,
            maxIdleTimeMS=45000
        )

        client.admin.command('ping')
        print(" - Connected to MongoDB")
    except (ServerSelectionTimeoutError, ConnectionFailure) as e:
        print(f" - Failed to connect to MongoDB: {e}")
        print("   - For Docker: Run '.\\run.ps1' (includes MongoDB)")
        print("   - For local: Install MongoDB and start mongod service")
        raise

    rawLayer(client)
    cleanLayer(client)
    aggregateLayer(client)

    if os.getenv("RUN_PERFORMANCE", "true").lower() in ("1", "true", "yes"):
        performanceLayer(client)
