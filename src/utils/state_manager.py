""" GCS State Manager class"""

import json
from google.cloud import storage
from src import config
from src.utils.logger import setup_logger

logger = setup_logger()

class StateManager:
    """
    Manages the state of the data pipeline, specifically tracking the last processed date.
    """
    def __init__(self):
        """Initializes the StateManager with GCS client and bucket."""
        self.storage_client = storage.Client(project=config.PROJECT_ID)
        self.bucket = self.storage_client.bucket(config.BUCKET_NAME)
        self.blob = self.bucket.blob(config.STATE_FILE_PATH)

    def get_last_processed_date(self):
        """Reads the last processed date from GCS state file."""
        if not self.blob.exists():
            logger.info("State file not found. Assuming no previous runs.")
            return None

        try:
            content = self.blob.download_as_text()
            state = json.loads(content)
            return state.get('last_processed_date')
        except Exception as e:
            logger.error("Failed to read state file: %s", e)
            return None

    def update_state(self, run_date):
        """Updates the state file with the new run date."""
        state = {'last_processed_date': run_date}
        try:
            self.blob.upload_from_string(json.dumps(state), content_type='application/json')
            logger.info("State updated: last_processed_date = %s", run_date)
        except Exception as e:
            logger.error("Failed to update state file: %s", e)
            raise
