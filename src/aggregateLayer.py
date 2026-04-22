def aggregateLayer(client):
    print("Start Aggregate Layer.")

    # Access MongoDB 
    db = client['database']
    collection = db['collection']

    print("End Aggregate Layer.")