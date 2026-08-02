"""Main driver script for the CSV Ingestion Pipeline."""

from pipeline.reader import CSVReaderPipeline
from pipeline.exceptions import CSVEngineError

def run():
    target_csv = "data/employee.csv"
    
    try:
        pipeline = CSVReaderPipeline(file_path=target_csv, delimeter=",")
        
        print("\n--- STREAMING INGESTED DATA RECORDS ---")
        for record in pipeline.process_csv():
            print(f"Record Received: {record}")
            
    except CSVEngineError as e:
        print(f"\n[CRITICAL PIPELINE FAILURE]: {e}")

if __name__ == "__main__":
    run()