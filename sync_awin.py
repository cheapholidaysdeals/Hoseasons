import os
import requests
import gzip
import csv
import io
from supabase import create_client, Client

# 1. Initialize Supabase client
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(url, key)

AWIN_URL = os.environ.get("AWIN_FEED_URL")
TABLE_NAME = "Hoseasons" 
BATCH_SIZE = 1000 

def main():
    print("Downloading AWIN feed...")
    response = requests.get(AWIN_URL)
    response.raise_for_status()

    print("Decompressing and parsing GZIP...")
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        csv_content = gz.read().decode('utf-8-sig')
    
    reader = csv.DictReader(io.StringIO(csv_content))
    
    # --- NEW CLEANING STEP ---
    # We loop through the rows and convert empty strings to None (NULL)
    data = []
    for row in reader:
        for key, value in row.items():
            if value == "":
                row[key] = None
        data.append(row)
    # -------------------------
    
    total_rows = len(data)
    print(f"Parsed and cleaned {total_rows} rows from AWIN.")

    if total_rows == 0:
        print("No data found. Exiting to prevent accidental table wipe.")
        return

    # 2. Wipe the existing data
    print(f"Wiping existing Supabase table: {TABLE_NAME}...")
    # Using the direct delete method since your dataset is currently small and fast
    supabase.table(TABLE_NAME).delete().neq("id", -1).execute()

    # 3. Batch insert new data
    print("Uploading fresh data to Supabase...")
    for i in range(0, total_rows, BATCH_SIZE):
        batch = data[i : i + BATCH_SIZE]
        
        # Insert the batch into Supabase
        supabase.table(TABLE_NAME).insert(batch).execute()
        
        print(f"Inserted rows {i} to {i + len(batch)}")

    print("Sync complete! Your table is now fully updated.")

if __name__ == "__main__":
    main()
