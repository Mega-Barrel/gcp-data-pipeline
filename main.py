import sys
from src import ingest, transform, load

def main():
    print("Starting GCP Data Pipeline...")
    
    try:
        print("\n--- Step 1: Ingestion ---")
        ingest.run_ingestion()
        
        print("\n--- Step 2: Transformation ---")
        transform.run_transformation()
        
        print("\n--- Step 3: Loading ---")
        load.load_to_bigquery()
        
        print("\nPipeline finished successfully!")
        
    except Exception as e:
        print(f"\nPipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
