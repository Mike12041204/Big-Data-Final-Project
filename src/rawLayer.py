import pandas as pd
import os

def rawLayer(client):
    print("Start Raw Layer.")

    # Read the CSV into a DataFrame
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'testData.csv')
    df = pd.read_csv(csv_path)
    
    # Access MongoDB 
    db = client['database']
    collection = db['collection']
    
    # Write the DataFrame to MongoDB
    data_dict = df.to_dict('records')
    collection.insert_many(data_dict)

    print("End Raw Layer.")