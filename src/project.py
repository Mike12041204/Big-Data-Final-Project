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

    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/?directConnection=true")
    docker_uri = os.getenv(
        "DOCKER_MONGODB_URI",
        "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
    )

    try:
        print(f" - Attempting to connect to MongoDB at {mongodb_uri}")
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
        client.admin.command('ping')
        print(" - Connected to MongoDB")
    except (ServerSelectionTimeoutError, ConnectionFailure):
        print(" - Local MongoDB not available, trying Docker replica set...")
        try:
            client = MongoClient(docker_uri, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000)
            client.admin.command('ping')
            print(" - Connected to Docker MongoDB replica set")
        except (ServerSelectionTimeoutError, ConnectionFailure) as e:
            print(f" - Failed to connect to MongoDB: {e}")
            print(" - Make sure MongoDB is running:")
            print("   - For Docker: Run '.\\run.ps1' (includes MongoDB)")
            print("   - For local: Install MongoDB and start mongod service")
            raise

    # Clear existing data
    try:
        client['raw']['rawData'].delete_many({})
        print(" - Wiped database")
    except Exception as e:
        print(f" - Warning: Could not clear database: {e}")

    rawLayer(client)
    cleanLayer(client)
    aggregateLayer(client)

    if os.getenv("RUN_PERFORMANCE", "true").lower() in ("1", "true", "yes"):
        performanceLayer(client)