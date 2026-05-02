"""
Performance Testing Layer
Demonstrates query optimization through indexing
"""

import time
import logging
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def performanceLayer(client):
    """
    Demonstrates query performance improvement with indexing.
    Tests on cleaned taxi data.
    
    Real-world scenario: Analyzing trip patterns by date and location
    """
    
    db = client['raw']
    collection = db['cleanData']
    
    print("\n" + "="*60)
    print("BEGIN PERFORMANCE TESTING LAYER")
    print("="*60)
    
    # Get collection stats
    doc_count = collection.count_documents({})
    logger.info(f"Total documents in cleanData: {doc_count}")
    
    if doc_count == 0:
        logger.warning("No cleaned data to test. Run clean layer first.")
        print("END PERFORMANCE TESTING LAYER\n")
        return
    
    # ===== TEST 1: Query without index =====
    logger.info("\n--- TEST 1: Query WITHOUT Index ---")
    logger.info("Query: Find all trips with passenger_count = 1")
    
    # Ensure no index exists (drop if it does)
    try:
        collection.drop_index("passenger_count_1")
        logger.info("Dropped existing passenger_count index")
    except:
        pass  # Index didn't exist
    
    start = time.time()
    result_no_index = list(collection.find({"passenger_count": 1}).limit(1000))
    time_no_index = time.time() - start
    
    logger.info(f"   Found: {len(result_no_index)} documents")
    logger.info(f"   Time: {time_no_index:.4f} seconds")
    
    # ===== TEST 2: Query WITH index =====
    logger.info("\n--- TEST 2: Query WITH Index ---")
    logger.info("Creating index on passenger_count...")
    
    collection.create_index("passenger_count")
    logger.info("Index created successfully")
    
    start = time.time()
    result_with_index = list(collection.find({"passenger_count": 1}).limit(1000))
    time_with_index = time.time() - start
    
    logger.info(f"   Found: {len(result_with_index)} documents")
    logger.info(f"   Time: {time_with_index:.4f} seconds")
    
    # ===== PERFORMANCE COMPARISON =====
    improvement = ((time_no_index - time_with_index) / time_no_index * 100) if time_no_index > 0 else 0
    logger.info(f"\n--- Performance Summary ---")
    logger.info(f"Without Index: {time_no_index:.4f}s")
    logger.info(f"With Index:    {time_with_index:.4f}s")
    logger.info(f"Improvement:   {improvement:.2f}%")
    
    # ===== TEST 3: Complex query with compound index =====
    logger.info("\n--- TEST 3: Range Query (Date-based) ---")
    logger.info("Query: Find all trips by location_id with date range")
    
    # Drop old index
    try:
        collection.drop_index("pickup_location_id_1_pickup_datetime_1")
    except:
        pass
    
    # Test without index
    start = time.time()
    query_result = list(collection.find({
        "pickup_location_id": 161,
        "passenger_count": {"$gte": 1}
    }).limit(1000))
    time_no_compound = time.time() - start
    
    logger.info(f"   Without compound index: {time_no_compound:.4f}s")
    
    # Create compound index
    collection.create_index([("pickup_location_id", 1), ("passenger_count", 1)])
    logger.info("Compound index created on (pickup_location_id, passenger_count)")
    
    start = time.time()
    query_result = list(collection.find({
        "pickup_location_id": 161,
        "passenger_count": {"$gte": 1}
    }).limit(1000))
    time_with_compound = time.time() - start
    
    logger.info(f"   With compound index:    {time_with_compound:.4f}s")
    
    # ===== INDEX STATISTICS =====
    logger.info("\n--- Index Statistics ---")
    indexes = collection.list_indexes()
    for idx in indexes:
        logger.info(f"Index: {idx['name']} - Keys: {idx['key']}")
    
    # Save performance results to MongoDB
    performance_doc = {
        "test_name": "Query Performance Analysis",
        "query_type": "passenger_count filter",
        "metrics": {
            "without_index_ms": round(time_no_index * 1000, 2),
            "with_index_ms": round(time_with_index * 1000, 2),
            "improvement_percent": round(improvement, 2)
        },
        "compound_query": {
            "without_index_ms": round(time_no_compound * 1000, 2),
            "with_index_ms": round(time_with_compound * 1000, 2)
        },
        "explanation": "Indexes on frequently queried columns (passenger_count, pickup_location_id) "
                       "reduce scan time by allowing MongoDB to seek directly to matching records "
                       "instead of scanning every document.",
        "recommendation": "Create indexes on: pickup_location_id, dropoff_location_id, "
                         "passenger_count, and pickup_datetime for optimal query performance"
    }
    
    db['performance_results'].insert_one(performance_doc)
    logger.info("Performance results saved to 'performance_results' collection")
    
    print("="*60)
    print("END PERFORMANCE TESTING LAYER")
    print("="*60 + "\n")
