""" Method to transform and dump clean data into GCS bucket."""

import json
import pandas as pd
from google.cloud import storage
from . import config
from .utils.logger import setup_logger

logger = setup_logger()

class TransformationManager:
    """
    Handles the transformation of raw data and saving it back to GCS.
    """
    def __init__(self):
        """Initializes the TransformationManager with GCS client and bucket."""
        self.storage_client = storage.Client(project=config.PROJECT_ID)
        self.bucket = self.storage_client.bucket(config.BUCKET_NAME)

    def read_from_gcs(self, blob_name):
        """
        Reads a blob from GCS as a string.
        
        Args:
            blob_name (str): The path of the blob to read.
            
        Returns:
            str: The content of the blob.
            
        Raises:
            FileNotFoundError: If the blob does not exist.
        """
        logger.info("Reading from gs://%s/%s...", config.BUCKET_NAME, blob_name)
        blob = self.bucket.blob(blob_name)
        if not blob.exists():
             raise FileNotFoundError(f"File {blob_name} not found in bucket {config.BUCKET_NAME}")
        return blob.download_as_text()

    def flatten_data(self, raw_json_str, run_date):
        """
        Flattens the nested carts/products structure.
        
        Args:
            raw_json_str (str): The raw JSON string.
            run_date (str): The date of the run to add as ingestion_date.
            
        Returns:
            pd.DataFrame: Flattened DataFrame.
        """
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
                    'ingestion_date': run_date
                }
                flattened_rows.append(row)

        return pd.DataFrame(flattened_rows)

    def upload_ndjson_to_gcs(self, df, destination_blob_name):
        """
        Uploads DataFrame as NDJSON to GCS.
        
        Args:
            df (pd.DataFrame): The DataFrame to upload.
            destination_blob_name (str): The destination path in GCS.
        """
        logger.info("Uploading clean data to gs://%s/%s...", config.BUCKET_NAME, destination_blob_name)
        blob = self.bucket.blob(destination_blob_name)

        # Convert to NDJSON string
        ndjson_str = df.to_json(orient='records', lines=True)

        blob.upload_from_string(ndjson_str, content_type='application/json')
        logger.info("Upload complete: gs://%s/%s...", config.BUCKET_NAME, destination_blob_name)

    def run(self, run_date):
        """
        Main transformation flow.
        
        Args:
            run_date (str): The date for which to run the transformation (YYYY-MM-DD).
        """
        try:
            raw_path = config.get_raw_path(run_date)
            raw_data = self.read_from_gcs(raw_path)

            df = self.flatten_data(raw_data, run_date)
            logger.info("Transformed %s rows.", len(df))

            clean_path = config.get_clean_path(run_date)
            self.upload_ndjson_to_gcs(df, clean_path)
            logger.info("Transformation successful.")
        except Exception as e:
            logger.error("Transformation failed: %s", e)
            raise
