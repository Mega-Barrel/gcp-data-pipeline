""" Method to transform and dump clean data into GCS bucket."""

from datetime import datetime

import json
import pandas as pd
from google.cloud import storage
from . import config

def read_from_gcs(bucket_name, blob_name):
    """Reads a blob from GCS as a string."""
    print(f"Reading from gs://{bucket_name}/{blob_name}...")
    storage_client = storage.Client(project=config.PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_text()

def flatten_data(raw_json_str):
    """Flattens the nested carts/products structure."""
    data = json.loads(raw_json_str)
    carts = data.get('carts', [])

    flattened_rows = []
    for cart in carts:
        cart_id = cart.get('id')
        user_id = cart.get('userId')

        for product in cart.get('products', []):
            row = {
                'cart_id': cart_id,
                'user_id': user_id,
                'product_id': product.get('id'),
                'product_title': product.get('title'),
                'price': product.get('price'),
                'quantity': product.get('quantity'),
                'item_total': product.get('total'),
                'discount_percentage': product.get('discountPercentage'),
                'discounted_total': product.get('discountedTotal'),
                'ingestion_date': datetime.now().strftime("%Y-%m-%d")
            }
            flattened_rows.append(row)

    return pd.DataFrame(flattened_rows)

def upload_ndjson_to_gcs(df, bucket_name, destination_blob_name):
    """Uploads DataFrame as NDJSON to GCS."""
    print(f"Uploading clean data to gs://{bucket_name}/{destination_blob_name}...")
    storage_client = storage.Client(project=config.PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert to NDJSON string
    ndjson_str = df.to_json(orient='records', lines=True)

    blob.upload_from_string(ndjson_str, content_type='application/json')
    print(f"Upload complete: gs://{bucket_name}/{destination_blob_name}")

def run_transformation():
    """Main transformation flow."""
    try:
        raw_data = read_from_gcs(config.BUCKET_NAME, config.RAW_PATH)
        df = flatten_data(raw_data)
        print(f"Transformed {len(df)} rows.")
        upload_ndjson_to_gcs(df, config.BUCKET_NAME, config.CLEAN_PATH)
        print("Transformation successful.")
    except Exception as e:
        print(f"Transformation failed: {e}")
        raise

if __name__ == "__main__":
    run_transformation()
