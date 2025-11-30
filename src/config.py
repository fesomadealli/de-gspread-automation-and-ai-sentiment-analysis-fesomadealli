

import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    GOOGLE_SHEET_KEY = os.getenv("GOOGLE_SHEET_KEY")
    GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
    WORKSHEET_ID = os.getenv("WORKSHEET_ID")
    INPUT_SHEET_NAME = os.getenv("INPUT_SHEET_NAME", "raw_data")
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    