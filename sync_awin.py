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
TABLE_NAME = "Hoseasons" # IMPORTANT: Change this to your table name
BATCH_SIZE = 1000 # Supabase handles 1000-2000 rows per batch well

def main():
    print("Downloading AWIN feed...")
    response = requests.get(AWIN_URL)
    response.raise_for_status()

    print("Decompressing and parsing GZIP...")
    # Unzip the content
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        csv_content = gz.read().decode('utf-8')
    
    # Parse the CSV. AWIN uses commas, but double-check your specific feed.
    reader = csv.DictReader(io.StringIO(csv_content))
    data = [row for row in reader]
    
    total_rows = len(data)
    print(f"Parsed {total_rows} rows from AWIN.")

    if total_rows == 0:
        print("No data found. Exiting to prevent accidental table wipe.")
        return

    # 2. Wipe the existing data securely using the RPC we created
    print("Wiping existing Supabase table...")
    supabase.rpc("truncate_awin_table", {}).execute()

    # 3. Batch insert new data
    print("Uploading fresh data to Supabase...")
    for i in range(0, total_rows, BATCH_SIZE):
        batch = data[i : i + BATCH_SIZE]
        
        # AWIN columns often have spaces or characters that SQL doesn't like. 
        # Make sure your Supabase column names exactly match the AWIN CSV headers.
        supabase.table(TABLE_NAME).insert(batch).execute()
        
        print(f"Inserted rows {i} to {i + len(batch)}")

    print("Sync complete!")

if __name__ == "__main__":
    main()
