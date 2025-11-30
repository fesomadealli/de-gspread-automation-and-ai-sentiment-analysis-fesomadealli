

import pandas as pd
import gspread
from logger import setup_logger
from src.config import Config
import os

logger = setup_logger(__name__)

class Sheets:
    def __init__(self):
        
        self.client = gspread.service_account(filename='creds/service_account.json')
        self.sheet = self.client.open_by_key(key=Config.GOOGLE_SHEET_KEY)
        self.worksheet = self.sheet.get_worksheet_by_id(id=Config.WORKSHEET_ID)
        self.data = self.worksheet.get_all_records()

    def read_data(self, worksheet_name: str) -> pd.DataFrame:
        """Read data from the specified worksheet into a DataFrame."""
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cache_path = os.path.join(project_root, 'cached_data.csv')
        
        try:
            self.df = pd.read_csv(cache_path)
            logger.info("Data loaded from cached_data.csv")
            return self.df
        except FileNotFoundError:   
            logger.info(f"Reading data from worksheet: {worksheet_name}")
            self.worksheet = self.sheet.worksheet(worksheet_name)
            self.data = self.worksheet.get_all_records()
            self.df = pd.DataFrame(self.data)
            self.df.to_csv(cache_path, index=False)
            logger.info("Data cached to cached_data.csv")
            return self.df
        except Exception as e:
            logger.error(f"Error reading data: {e}")            
    
    def create_worksheet(self, spreadsheet, worksheet_name: str, rows: int = 1000, cols: int = 20):
        """Create a new worksheet if not exists (idempotent)."""
        try:
            return spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            return spreadsheet.add_worksheet(title=worksheet_name, rows=rows, cols=cols)

    def protect_worksheet(self, worksheet):
        """Protect worksheet from edits by other users."""
        body = {
                    "requests": [
                        {
                            "addProtectedRange": {
                                "protectedRange": {
                                    "range": {
                                        "sheetId": worksheet.id,
                                    }
                                }
                            }
                        }
                    ]
                }
        self.sheet.batch_update(body)
        
if __name__ == "__main__":
    sheets = Sheets() 
    test_ws = sheets.create_worksheet(sheets.sheet, "Test_Worksheet")
    print("Worksheet created or already exists.")
    sheets.protect_worksheet(test_ws)
    print("Worksheet protected.")