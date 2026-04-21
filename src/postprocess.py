def postprocess(client):
    print("Start postprocessing.")

    # Access MongoDB 
    db = client['database']
    collection = db['collection']