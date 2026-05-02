import json
import pandas as pd
import os
from .models import RawData


def rawLayer(client):
    print("BEGIN RAW LAYER")

    csv_filename = os.getenv("DATA_FILE", "taxi_trip_data.csv")
    csv_path = os.path.join(os.path.dirname(__file__), '..', csv_filename)
    if not os.path.exists(csv_path):
        fallback_path = os.path.join(os.path.dirname(__file__), '..', 'testData.csv')
        if os.path.exists(fallback_path):
            print(f" - Warning: {csv_filename} not found. Falling back to sample file: {fallback_path}")
            csv_path = fallback_path
        else:
            raise FileNotFoundError(f"Could not find dataset at {csv_path} or fallback {fallback_path}")

    print(f" - CSV loaded: {csv_path}")

    raw_collection = client['raw']['rawData']
    raw_collection.delete_many({})

    chunksize = 200_000
    inserted = 0
    for idx, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize)):
        records = [RawData.model_validate(r).to_mongo() for r in chunk.to_dict('records')]
        if records:
            raw_collection.insert_many(records)
            inserted += len(records)
            print(f" - Inserted chunk {idx + 1}: {len(records)} records (total {inserted})")

    if inserted > 0:
        sample = raw_collection.find_one({}, {'_id': 0})
        print(f" - Total inserted records: {inserted}")
        print(f" - Example record:\n{json.dumps(sample, indent=4, default=str)}")
    else:
        print(" - No records inserted")

    print("END RAW LAYER")