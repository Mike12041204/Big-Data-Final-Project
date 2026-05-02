def aggregateLayer(client):
    print("Start Aggregate Layer.")

    # Access MongoDB collections
    raw_db = client['raw']
    clean_db = client['cleanData']

    # Get data from clean collection
    clean_data = list(clean_db.find({}, {"_id": 0}))
    print(f" - Found {len(clean_data)} cleaned records")

    if clean_data:
        # Example aggregations
        print(" - Performing aggregations...")

        # 1. Count by category
        categories = {}
        total_value = 0

        for record in clean_data:
            category = record.get('category', 'Unknown')
            value = record.get('value', 0)
            categories[category] = categories.get(category, 0) + 1
            total_value += value

        print(f" - Total records: {len(clean_data)}")
        print(f" - Total value: {total_value}")
        print(f" - Categories: {categories}")

        # 2. Average value per category
        for category, count in categories.items():
            avg_value = total_value / len(categories) if categories else 0
            print(f" - {category}: {count} records")

        # 3. Save aggregated results (optional)
        aggregated_db = client['aggregated']
        aggregated_db.drop_collection('summary')

        summary = {
            'total_records': len(clean_data),
            'total_value': total_value,
            'categories': categories,
            'average_value': total_value / len(clean_data) if clean_data else 0
        }

        aggregated_db['summary'].insert_one(summary)
        print(" - Saved aggregated summary")

    print("End Aggregate Layer.")