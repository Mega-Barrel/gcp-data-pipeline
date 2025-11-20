
import sys
from datetime import datetime
from src.utils.logger import setup_logger
from src.utils.state_manager import StateManager
from src.ingest import IngestionManager
from src.transform import TransformationManager
from src.load import LoadManager

logger = setup_logger()

def run_pipeline():
    """
    Orchestrates the entire data pipeline process:
    1. Checks state to avoid duplicate runs.
    2. Ingests data from API.
    3. Transforms data.
    4. Loads data to BigQuery.
    5. Updates state.
    """
    logger.info("-" * 30)
    logger.info("Starting GCP Data Pipeline...")

    # Initialize managers
    state_manager = StateManager()
    ingestion_manager = IngestionManager()
    transformation_manager = TransformationManager()
    load_manager = LoadManager()

    current_date = datetime.now().strftime("%Y-%m-%d")

    last_processed_date = state_manager.get_last_processed_date()
    logger.info(f"Last processed date: {last_processed_date}")

    if last_processed_date == current_date:
        logger.info(f"Data for {current_date} already processed. Skipping.")
    else:
        try:
            logger.info("Step 1: Ingestion")
            ingestion_manager.run(current_date)

            logger.info("Step 2: Transformation")
            transformation_manager.run(current_date)

            logger.info("Step 3: Load")
            load_manager.run(current_date)

            logger.info("Step 4: Update State")
            state_manager.update_state(current_date)

            logger.info("Pipeline finished successfully.")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
