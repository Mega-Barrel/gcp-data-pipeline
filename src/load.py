""" Method to load clean data into BigQuery table."""

from google.cloud import bigquery
from . import config

def load_to_bigquery():
    """Loads NDJSON from GCS to BigQuery."""
    print(f"Loading data into BigQuery table {config.PROJECT_ID}.{config.BQ_DATASET}.{config.BQ_TABLE}...")

    client = bigquery.Client(project=config.PROJECT_ID)

    # Create dataset if it doesn't exist
    dataset_ref = client.dataset(config.BQ_DATASET)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {config.BQ_DATASET} already exists.")
    except Exception:
        print(f"Creating dataset {config.BQ_DATASET}...")
        client.create_dataset(dataset_ref)

    table_ref = dataset_ref.table(config.BQ_TABLE)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    uri = f"gs://{config.BUCKET_NAME}/{config.CLEAN_PATH}"

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )

    print(f"Starting job {load_job.job_id}...")
    load_job.result()
    print("Job finished.")

    destination_table = client.get_table(table_ref)
    print(f"Loaded {destination_table.num_rows} rows.")

if __name__ == "__main__":
    load_to_bigquery()
