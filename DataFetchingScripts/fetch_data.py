import requests
import json
import time

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
FILENAME = "nyc_311_raw_data.jsonl"
CHUNK_SIZE = 50000            
TOTAL_GOAL = 1500000           
JUMP_MULTIPLIER = 10           

print(f"Starting download for {TOTAL_GOAL} rows:")

with open(FILENAME, 'w', encoding='utf-8') as f:
    rows_collected = 0
    
    for offset_step in range(0, TOTAL_GOAL, CHUNK_SIZE):
        actual_offset = offset_step * JUMP_MULTIPLIER
        
        params = {
            "$limit": CHUNK_SIZE,
            "$offset": actual_offset,
            "$order": "created_date DESC" 
        }
        
        try:
            response = requests.get(BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            if not data:
                print("No more data available from API.")
                break

            for record in data:
                f.write(json.dumps(record) + "\n")
            
            rows_collected += len(data)
            print(f"Collected {rows_collected:,} / {TOTAL_GOAL:,} (Current Offset: {actual_offset:,})")

            time.sleep(0.5)

        except Exception as e:
            print(f"Error at offset {actual_offset}: {e}")
            break

print(f"\nDone! {rows_collected:,} rows saved to {FILENAME}")