def aggregateLayer(client):
    print("Start Aggregate Layer.")

    collection = client['raw']['cleanData']
    total_trips = collection.count_documents({})
    print(f" - Cleaned trip count: {total_trips}")

    if total_trips == 0:
        print(" - No cleaned taxi data available for aggregation")
        print("End Aggregate Layer.")
        return

    metrics_pipeline = [
        {
            "$group": {
                "_id": None,
                "avg_trip_distance": {"$avg": "$trip_distance"},
                "avg_fare_amount": {"$avg": "$fare_amount"},
                "avg_tip_percent": {"$avg": "$tip_percent"},
                "avg_speed_mph": {"$avg": "$speed_mph"},
                "total_revenue": {"$sum": "$total_amount"},
                "max_trip_distance": {"$max": "$trip_distance"},
                "min_trip_distance": {"$min": "$trip_distance"}
            }
        }
    ]

    metrics = list(collection.aggregate(metrics_pipeline))
    summary = {
        'total_trips': total_trips,
        'avg_trip_distance': None,
        'avg_fare_amount': None,
        'avg_tip_percent': None,
        'avg_speed_mph': None,
        'total_revenue': None,
        'max_trip_distance': None,
        'min_trip_distance': None,
        'top_pickup_locations': [],
        'top_payment_types': [],
        'trips_by_hour': []
    }

    if metrics:
        result = metrics[0]
        summary.update({
            'avg_trip_distance': result.get('avg_trip_distance'),
            'avg_fare_amount': result.get('avg_fare_amount'),
            'avg_tip_percent': result.get('avg_tip_percent'),
            'avg_speed_mph': result.get('avg_speed_mph'),
            'total_revenue': result.get('total_revenue'),
            'max_trip_distance': result.get('max_trip_distance'),
            'min_trip_distance': result.get('min_trip_distance')
        })

    print(" - Aggregated metrics:")
    print(f"   avg_trip_distance: {summary['avg_trip_distance']}")
    print(f"   avg_fare_amount: {summary['avg_fare_amount']}")
    print(f"   avg_tip_percent: {summary['avg_tip_percent']}")
    print(f"   avg_speed_mph: {summary['avg_speed_mph']}")
    print(f"   total_revenue: {summary['total_revenue']}")

    top_pickup_pipeline = [
        {"$group": {"_id": "$pickup_location_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    top_pickups = [
        {"pickup_location_id": doc['_id'], "count": doc['count']}
        for doc in collection.aggregate(top_pickup_pipeline)
        if doc['_id'] is not None
    ]
    summary['top_pickup_locations'] = top_pickups
    print(f" - Top pickup locations: {top_pickups}")

    payment_pipeline = [
        {"$group": {"_id": "$payment_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    top_payment = [
        {"payment_type": doc['_id'], "count": doc['count']}
        for doc in collection.aggregate(payment_pipeline)
        if doc['_id'] is not None
    ]
    summary['top_payment_types'] = top_payment
    print(f" - Top payment types: {top_payment}")

    if collection.count_documents({"pickup_hour": {"$exists": True}}) > 0:
        hour_pipeline = [
            {"$group": {"_id": "$pickup_hour", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        summary['trips_by_hour'] = [
            {"pickup_hour": doc['_id'], "count": doc['count']}
            for doc in collection.aggregate(hour_pipeline)
        ]
        print(f" - Trips by hour: {summary['trips_by_hour'][:8]}")

    aggregated_db = client['aggregated']
    aggregated_db.drop_collection('summary')
    aggregated_db['summary'].insert_one(summary)
    print(" - Saved aggregated summary")
    print("End Aggregate Layer.")