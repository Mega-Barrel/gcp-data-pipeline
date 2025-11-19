# GCP Data Pipeline

This project implements a data pipeline that fetches data from [DummyJSON Carts API](https://dummyjson.com/carts), flattens the product data, and loads it into Google BigQuery.

## Architecture

1.  **Ingest**: Fetches JSON from API -> Saves to GCS (Raw).
2.  **Transform**: Reads Raw JSON -> Flattens `products` -> Saves to GCS (Clean NDJSON).
3.  **Load**: Loads Clean NDJSON -> BigQuery Table `carts_flattened`.

## Prerequisites

- Python 3.9+
- Google Cloud Project with BigQuery and Storage enabled.
- Authenticated `gcloud` CLI or Service Account key (`credentials.json`).

## Setup

1.  **Authentication**:
    - Place your Service Account key file named `credentials.json` in the root of this project.
    - The application will automatically detect and use it.
    - *Note: Do not commit `credentials.json` to version control!*

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  Set Environment Variables (Optional, defaults provided in `src/config.py`):
    ```bash
    export GCP_PROJECT_ID="your-project-id"
    export GCS_BUCKET_NAME="your-bucket-name"
    export BQ_DATASET="retail_data"
    ```

## Running the Pipeline

Run the main script:

```bash
python main.py
```
