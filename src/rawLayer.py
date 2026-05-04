<<<<<<< HEAD
import json
import pandas as pd
import os
import gc
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
    try:
        raw_collection.delete_many({})
        print(" - Cleared existing raw data")
    except Exception as e:
        print(f" - Warning: Could not clear existing raw data: {e}")
        print(" - Proceeding with data insertion...")

    chunksize = 50_000
    inserted = 0
    for idx, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize)):
        try:
            records = [RawData.model_validate(r).to_mongo() for r in chunk.to_dict('records')]
            if records:
                raw_collection.insert_many(records)
                inserted += len(records)
                print(f" - Inserted chunk {idx + 1}: {len(records)} records (total {inserted})")
            
            # Aggressive garbage collection
            del chunk, records
            gc.collect()
        except Exception as e:
            print(f" - Warning: Error processing chunk {idx + 1}: {e}")
            continue

    if inserted > 0:
        sample = raw_collection.find_one({}, {'_id': 0})
        print(f" - Total inserted records: {inserted}")
        print(f" - Example record:\n{json.dumps(sample, indent=4, default=str)}")
    else:
        print(" - No records inserted")

=======
import json
import pandas as pd
import os
import gc
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
    try:
        raw_collection.delete_many({})
        print(" - Cleared existing raw data")
    except Exception as e:
        print(f" - Warning: Could not clear existing raw data: {e}")
        print(" - Proceeding with data insertion...")

    chunksize = 50_000
    inserted = 0
    for idx, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize)):
        try:
            records = [RawData.model_validate(r).to_mongo() for r in chunk.to_dict('records')]
            if records:
                raw_collection.insert_many(records)
                inserted += len(records)
                print(f" - Inserted chunk {idx + 1}: {len(records)} records (total {inserted})")
            
            # Aggressive garbage collection
            del chunk, records
            gc.collect()
        except Exception as e:
            print(f" - Warning: Error processing chunk {idx + 1}: {e}")
            continue

    if inserted > 0:
        sample = raw_collection.find_one({}, {'_id': 0})
        print(f" - Total inserted records: {inserted}")
        print(f" - Example record:\n{json.dumps(sample, indent=4, default=str)}")
    else:
        print(" - No records inserted")

>>>>>>> 31a89514fe5c607ba63bb0574cb71393aba7e6a1
    print("END RAW LAYER")