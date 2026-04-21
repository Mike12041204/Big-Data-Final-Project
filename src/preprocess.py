import pandas as pd
import os

def preprocess(client):
    print("Start preprocessing.")

    # Read the CSV into a DataFrame
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'testData.csv')
    df = pd.read_csv(csv_path)
    
    # Access MongoDB 
    db = client['database']
    collection = db['collection']

    # Reset database
    collection.delete_many({})
    
    # Write the DataFrame to MongoDB
    data_dict = df.to_dict('records')
    collection.insert_many(data_dict)