import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Authentication: Load credentials.json if it exists
# We look for 'credentials.json' in the project root (one level up from this file's directory)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "credentials.json")

if os.path.exists(CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
    print(f"Using credentials from: {CREDENTIALS_PATH}")
else:
    # Fallback to default auth (gcloud auth application-default login)
    pass

# GCP Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-project-id")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", f"{PROJECT_ID}-data-pipeline")
BQ_DATASET = os.getenv("BQ_DATASET", "retail_data")
BQ_TABLE = "carts_flattened"
REGION = os.getenv("GCP_REGION", "US")

# API Configuration
API_URL = "https://dummyjson.com/carts"

# Paths
DATE_STR = datetime.now().strftime("%Y-%m-%d")
RAW_PATH = f"raw/carts/{DATE_STR}/carts_raw.json"
CLEAN_PATH = f"clean/carts/{DATE_STR}/carts_flattened.json"

def get_gcs_path(bucket, blob_path):
    return f"gs://{bucket}/{blob_path}"
