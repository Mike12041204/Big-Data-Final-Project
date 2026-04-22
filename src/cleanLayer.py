def cleanLayer(client):
    print("Start Clean Layer.")

    # Access MongoDB 
    db = client['database']
    collection = db['collection']

    print("End Clean Layer.")