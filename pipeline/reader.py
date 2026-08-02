import logging
from pipeline.exceptions import CorruptRowError
from typing import List,Dict, Generator
from pathlib import Path

logging.basicConfig(level=logging.INFO,format='%(asctime)s-%(levelname)s-%(message)s',handlers=[logging.FileHandler('logs/pipeline.log'),logging.StreamHandler()])

class CSVReaderPipeline:
    """ production grade csv reader pipeline"""

    def __init__(self,file_path:str,delimeter:str=","):
        self.file_path=file_path
        self.delimeter=delimeter
        self.processed_rows=0
        self.failedrows=0

        #ensuring log directory exists
        Path("logs").mkdir(parents=True,exist_ok=True)
        logging.info(f'Initialized CSVReaderPipeline with file_path:{self.file_path} and delimeter: {self.delimeter}')


    def validate_file_exists(self)->bool:
        """ validate if the file exists"""
        try:
            with open(self.file_path,'r') as file:
                logging.info(f'File {self.file_path} exists and is accessible')
                return True
        except FileNotFoundError:
            return False

    def process_csv(self) -> Generator[Dict[str, str], None, None]:
        """Streams CSV line-by-line using a generator to save memory."""
        if not self.validate_file_exists():
            raise FileNotFoundError(f"Cannot process missing file: {self.file_path}")

        logging.info("Starting CSV stream ingestion...")

        with open(self.file_path, mode="r", encoding="utf-8") as file:
            # Read header row
            header_line = file.readline().strip()
            if not header_line:
                logging.warning("CSV file appears to be empty!")
                return
            
            headers = [h.strip() for h in header_line.split(self.delimeter)]
            expected_cols = len(headers)
            logging.info(f"Schema detected. Columns ({expected_cols}): {headers}")

            # Stream remaining rows line-by-line
            for line_no, line in enumerate(file, start=2):
                clean_line = line.strip()
                if not clean_line:
                    continue  # Skip empty lines

                values = [v.strip() for v in clean_line.split(self.delimeter)]

                try:
                    # Data Quality Check: Match column count
                    if len(values) != expected_cols:
                        raise CorruptRowError(
                            f"Line {line_no}: Expected {expected_cols} columns, but got {len(values)}."
                        )
                    
                    # Map headers to values
                    row_dict = dict(zip(headers, values))
                    self.processed_rows += 1
                    yield row_dict

                except CorruptRowError as err:
                    self.failed_rows += 1
                    logging.warning(f"[DATA QUALITY ALERT] {err}")
                    continue  # Skip corrupted row and continue ingestion

        logging.info(
            f"Ingestion Finished! Total Successful: {self.processed_rows} | Total Failed: {self.failedrows}"
        )