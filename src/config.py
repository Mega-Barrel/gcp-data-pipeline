""" GCP Configuration File"""

import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "credentials.json")

if os.path.exists(CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
    print(f"Using credentials from: {CREDENTIALS_PATH}")

# GCP Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-project-id")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", f"{PROJECT_ID}-data-pipeline")
BQ_DATASET = os.getenv("BQ_DATASET", "retail_data")
BQ_TABLE = "mart_carts"
REGION = os.getenv("GCP_REGION", "US")

# API Configuration
API_URL = "https://dummyjson.com/carts"

# Paths
RAW_PATH_TEMPLATE = "raw/carts/carts_raw_{date}.json"
CLEAN_PATH_TEMPLATE = "clean/carts/carts_flattened_{date}.json"
STATE_FILE_PATH = "state/pipeline_state.json"

def get_raw_path(date_str):
    """
    Generates the raw data file path based on the date.

    Args:
        date_str (str): Date string in YYYY-MM-DD format.

    Returns:
        str: Formatted path for raw data file.
    """
    # User wants filenames like carts_raw_2025_11_19
    formatted_date = date_str.replace('-', '_')
    return RAW_PATH_TEMPLATE.format(date=formatted_date)

def get_clean_path(date_str):
    """
    Generates the clean data file path based on the date.

    Args:
        date_str (str): Date string in YYYY-MM-DD format.

    Returns:
        str: Formatted path for clean data file.
    """
    formatted_date = date_str.replace('-', '_')
    return CLEAN_PATH_TEMPLATE.format(date=formatted_date)

def get_gcs_path(bucket, blob_path):
    """
    Constructs a full GCS URI.

    Args:
        bucket (str): Bucket name.
        blob_path (str): Path to the blob.

    Returns:
        str: Full GCS URI (gs://bucket/path).
    """
    return f"gs://{bucket}/{blob_path}"
