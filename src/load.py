""" Method to load clean data into BigQuery table."""

from google.cloud import bigquery
from . import config
from .utils.logger import setup_logger

logger = setup_logger()

class LoadManager:
    """
    Handles the loading of transformed data into BigQuery.
    """
    def __init__(self):
        """Initializes the LoadManager with BigQuery client."""
        self.client = bigquery.Client(project=config.PROJECT_ID)

    def load_to_bigquery(self, source_uri):
        """
        Loads NDJSON from GCS to BigQuery.
        
        Args:
            source_uri (str): The GCS URI of the source data.
        """
        logger.info("Loading data into BigQuery table %s.%s.%s...", config.PROJECT_ID, config.BQ_DATASET, config.BQ_TABLE)

        # Create dataset if it doesn't exist
        dataset_ref = self.client.dataset(config.BQ_DATASET)
        try:
            self.client.get_dataset(dataset_ref)
            logger.info("Dataset %s already exists.", config.BQ_DATASET)
            logger.info("Skipping data loading phase.")
        except Exception:
            logger.info("Creating dataset %s...", config.BQ_DATASET)
            self.client.create_dataset(dataset_ref)

        table_ref = dataset_ref.table(config.BQ_TABLE)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        load_job = self.client.load_table_from_uri(
            source_uri,
            table_ref,
            job_config=job_config
        )

        logger.info("Starting job %s", load_job.job_id)
        load_job.result()
        logger.info("Job finished.")

        destination_table = self.client.get_table(table_ref)
        logger.info("Loaded %s rows.", destination_table.num_rows)

    def run(self, run_date):
        """
        Main load flow.
        
        Args:
            run_date (str): The date for which to run the load (YYYY-MM-DD).
        """
        try:
            clean_path = config.get_clean_path(run_date)
            uri = f"gs://{config.BUCKET_NAME}/{clean_path}"
            self.load_to_bigquery(uri)
            logger.info("Load successful.")
        except Exception as e:
            logger.error("Load failed: %s", e)
            raise
