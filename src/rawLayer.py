import pandas as pd
import os
from .models import RawData

def rawLayer(client):
    print("BEGIN RAW LAYER")

    csv_path = os.path.join(os.path.dirname(__file__), '..', 'testData.csv')
    df = pd.read_csv(csv_path)

    print(f" - CSV loaded: {csv_path}")

    records = [RawData.model_validate(r).to_mongo() for r in df.to_dict('records')]
    client['raw']['rawData'].insert_many(records)
        
    print(f" - Inserted records: {len(records)}")
    print("END RAW LAYER")