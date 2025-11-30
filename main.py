

from src.utils import Sheets
from src.etl import ETL
from src.analysis import add_ai_columns, write_processed

def run_pipeline(dataset_path: str):
    sheets = Sheets()
    etl = ETL(sheets.sheet)

    raw_df = etl.load_raw_data(dataset_path)
    staging_df = etl.stage_data()
    ai_df = add_ai_columns(staging_df)
    processed_data = etl.process_data(df=ai_df)
    processed_df = write_processed(sheets.sheet, processed_data)
    
    return processed_df #raw_df, staging_df


if __name__ == "__main__":
    run_pipeline("cached_data.csv")
