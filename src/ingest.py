""" Method to ingest raw JSON data from API and dumps into GCS bucket"""

import json
import requests
from google.cloud import storage
from . import config
from .utils.logger import setup_logger

logger = setup_logger()

class IngestionManager:
    """
    Handles the ingestion of data from external APIs to Google Cloud Storage.
    """
    def __init__(self):
        """Initializes the IngestionManager with GCS client and bucket."""
        self.storage_client = storage.Client(project=config.PROJECT_ID)
        self.bucket = self.storage_client.bucket(config.BUCKET_NAME)

    def fetch_data(self, url):
        """
        Fetches data from the given API URL.

        Args:
            url (str): The API endpoint URL.

        Returns:
            dict: JSON response from the API.

        Raises:
            requests.RequestException: If the API call fails.
        """
        logger.info("Fetching data from %s...", url)
        try:
            response = requests.get(url, timeout=500)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error("Error fetching data: %s", e)
            raise

    def upload_to_gcs(self, data, destination_blob_name):
        """
        Uploads data string to Google Cloud Storage.

        Args:
            data (dict): The data to upload (will be converted to JSON).
            destination_blob_name (str): The path in the GCS bucket.
        """
        logger.info("Uploading data to gs://%s/%s...", config.BUCKET_NAME, destination_blob_name)
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        logger.info("Upload complete: gs://%s/%s", config.BUCKET_NAME, destination_blob_name)

    def run(self, run_date):
        """
        Main ingestion flow.

        Args:
            run_date (str): The date for which to run the ingestion (YYYY-MM-DD).
        """
        try:
            data = self.fetch_data(config.API_URL)
            destination_path = config.get_raw_path(run_date)
            self.upload_to_gcs(data, destination_path)
            logger.info("Ingestion successful.")
        except Exception as e:
            logger.error("Ingestion failed: %s", e)
            raise
