import os
import requests
import gzip
import csv
import io
from supabase import create_client, Client

# 1. Initialize Supabase client using GitHub Secrets
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(url, key)

AWIN_URL = os.environ.get("AWIN_FEED_URL")
# EXACT match to your Supabase table name (Case-Sensitive)
TABLE_NAME = "Hoseasons" 
BATCH_SIZE = 1000 

def main():
    print("Downloading AWIN feed...")
    response = requests.get(AWIN_URL)
    response.raise_for_status()

    print("Decompressing and parsing GZIP...")
    # Unzip the content. 'utf-8-sig' safely removes hidden characters (BOM) from AWIN headers
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        csv_content = gz.read().decode('utf-8-sig')
    
    # Parse the CSV.
    reader = csv.DictReader(io.StringIO(csv_content))
    data = [row for row in reader]
    
    total_rows = len(data)
    print(f"Parsed {total_rows} rows from AWIN.")

    if total_rows == 0:
        print("No data found. Exiting to prevent accidental table wipe.")
        return

    # 2. Wipe the existing data securely using the RPC function we updated
    print(f"Wiping existing Supabase table: {TABLE_NAME}...")
    supabase.rpc("truncate_awin_table", {}).execute()

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
