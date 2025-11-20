""" Method to ingest raw JSON data from API and dumps into GCS bucket"""

import json
import requests
from google.cloud import storage

from . import config

def fetch_data(url):
    """Fetches data from the given API URL."""
    print(f"Fetching data from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def upload_to_gcs(data, bucket_name, destination_blob_name):
    """Uploads data string to Google Cloud Storage."""
    print(f"Uploading data to gs://{bucket_name}/{destination_blob_name}...")
    storage_client = storage.Client(project=config.PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(json.dumps(data), content_type='application/json')
    print(f"Upload complete: gs://{bucket_name}/{destination_blob_name}")

def run_ingestion():
    """Main ingestion flow."""
    try:
        data = fetch_data(config.API_URL)
        upload_to_gcs(data, config.BUCKET_NAME, config.RAW_PATH)
        print("Ingestion successful.")
    except Exception as e:
        print(f"Ingestion failed: {e}")
        raise

if __name__ == "__main__":
    run_ingestion()
