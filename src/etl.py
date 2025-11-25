

import pandas as pd
from logger import setup_logger
from src.utils import Sheets

logger = setup_logger(__name__)

class ETL:
    def __init__(self, spreadsheet):
        """
        Initialize ETL with a connected spreadsheet object.
        """
        self.spreadsheet = spreadsheet
        self.sheets = Sheets()

    def load_raw_data(self, dataset_path: str) -> pd.DataFrame:
        """
        Load the first 200 rows from the dataset into a 'raw_data' worksheet.
        """
        logger.info("Loading raw dataset...")
        
        df = pd.read_csv(dataset_path).head(200)
        df.fillna("", inplace=True)
        worksheet = self.sheets.create_worksheet(self.spreadsheet, "raw_data", 
                                                 rows=len(df)+1, cols=len(df.columns))
        
        # Write data to worksheet
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        logger.info("Raw data written to 'raw_data' worksheet")

        return df

    def stage_data(self) -> pd.DataFrame:
        """
        Pull data from 'raw_data', clean it, and load into 'staging' worksheet.
        """
        logger.info("Staging data from 'raw_data' worksheet...")
        raw_ws = self.spreadsheet.worksheet("raw_data")
        records = raw_ws.get_all_records()
        df = pd.DataFrame(records)

        # Cleaning: drop blanks, standardize text
        df = df.dropna().map(lambda x: str(x).strip().lower() if isinstance(x, str) else x)

        # Create or get staging worksheet
        staging_ws = self.sheets.create_worksheet(self.spreadsheet, "staging", 
                                                  rows=len(df)+1, cols=len(df.columns))
        
        # Write cleaned data
        values = [df.columns.values.tolist()] + df.values.tolist()
        staging_ws.update(values)
        logger.info("Cleaned data written to 'staging' worksheet")

        return df
