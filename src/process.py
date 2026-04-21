def process(client):
    print("Start processing.")

    # Access MongoDB 
    db = client['database']
    collection = db['collection']