

import pandas as pd
from logger import setup_logger
from src.utils import Sheets
from src import analysis

logger = setup_logger(__name__)

class ETL:
    def __init__(self, spreadsheet):
        """
        Initialize ETL with a connected spreadsheet object.
        """
        self.spreadsheet = spreadsheet
        self.sheets = Sheets()
        self.client = analysis.groq_client
        self.batch_size = analysis.Config.BATCH_SIZE
        

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
        df = df.dropna().map(lambda x: str(x).strip().lower() if isinstance(x, str) else x)
        staging_ws = self.sheets.create_worksheet(self.spreadsheet, "staging", 
                                                  rows=len(df)+1, cols=len(df.columns))
        values = [df.columns.values.tolist()] + df.values.tolist()
        staging_ws.update(values)
        logger.info("Cleaned data written to 'staging' worksheet")

        return df

    def process_data(
        self,
        df: pd.DataFrame = None,
        text_col: str = "Review Text",
        class_col: str = "Class Name"
        ) -> pd.DataFrame:
        """
        Run AI sentiment analysis in batches, add action flags,
        and write processed results.
        """
        logger.info("Processing data with AI sentiment analysis...")

        # Load staging data
        batch_size = self.batch_size

        sentiments, summaries = [], []
        total_rows = len(df)
        logger.info(f"Total rows to process: {total_rows}")
        logger.info(f"Batch size: {batch_size}")
        
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch_df = df.iloc[start:end]
            logger.info(f"Processing batch {start} -> {end - 1}")
            batch_sentiments = []
            batch_summaries = []

            for text in batch_df[text_col]:
                sentiment, summary = analysis.analyzer(
                    analysis.preprocess_review(text),
                    self.client
                )
                batch_sentiments.append(sentiment)
                batch_summaries.append(summary)
            sentiments.extend(batch_sentiments)
            summaries.extend(batch_summaries)

        # Attach AI results
        df["AI Sentiment"] = sentiments
        df["AI Summary"] = summaries

        # Apply action flag logic
        df = analysis.add_action_flag(df)

        # Write final processed data
        analysis.write_processed(self.spreadsheet, df)
        logger.info("Processed data written to 'processed' worksheet")

        # Compute analytics
        breakdown = analysis.sentiment_breakdown(df, class_col=class_col)
        extremes = analysis.sentiment_extremes(breakdown, class_col=class_col)
        logger.info(f"Sentiment extremes: {extremes}")
        analysis.plot_sentiment_breakdown(breakdown, class_col=class_col)
        logger.info(f"Plot saved to 'plots' directory")
        
        return df