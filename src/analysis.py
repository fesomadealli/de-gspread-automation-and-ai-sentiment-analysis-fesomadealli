

import pandas as pd
from src.utils import Sheets
from src.config import Config
from groq import Groq
import re
import unicodedata
import json
import os
import time
import matplotlib.pyplot as plt
from logger import setup_logger
from datetime import datetime

logger = setup_logger(__name__)


sheet =  Sheets()
groq_client = Groq(api_key=Config.GROQ_API_KEY)

def add_ai_columns(df: pd.DataFrame):
    """Add placeholder columns for AI results."""
    df["AI Sentiment"] = ""
    df["AI Summary"] = ""
    df["Action Needed?"] = ""
    return df

def write_processed(spreadsheet, df: pd.DataFrame):
    """Write processed dataframe to Google Sheet."""
    
    sheet_name = "processed"
    try:
        processed_ws = spreadsheet.worksheet(sheet_name)
        logger.info("Worksheet 'processed' exists.")
    except Exception as e:
        processed_ws = sheet.create_worksheet(spreadsheet, 
                                              sheet_name, 
                                              rows=len(df)+1, 
                                              cols=len(df.columns))
    processed_ws.update([df.columns.values.tolist()] + df.values.tolist())
    sheet.protect_worksheet(processed_ws)

def add_action_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'Action Needed?' flag based on sentiment.
    Rule: If sentiment == 'negative' → 'Yes', else 'No'.
    """
    if "AI Sentiment" not in df.columns:
        raise ValueError("DataFrame must contain 'AI Sentiment' column")

    df["Action Needed?"] = df["AI Sentiment"].apply(
        lambda s: "Yes" if str(s).lower() == "negative" else "No"
    )
    return df

# PERFORM ANALYSIS FUNCTIONS
def sentiment_breakdown(df: pd.DataFrame, class_col: str = "Class Name") -> pd.DataFrame:
    """
    Compute percentage breakdown of sentiments per clothing class.
    Returns a DataFrame with percentages.
    """
    if "AI Sentiment" not in df.columns or class_col not in df.columns:
        raise ValueError("DataFrame must contain 'AI Sentiment' and clothing class column")
    counts = df.groupby([class_col, "AI Sentiment"]).size().reset_index(name="count")
    total_counts = counts.groupby(class_col)["count"].transform("sum")
    counts["percentage"] = (counts["count"] / total_counts) * 100

    return counts

def sentiment_extremes(breakdown_df: pd.DataFrame, class_col: str = "Class Name"):
    """
    Identify clothing class with highest positive, negative, and neutral sentiment.
    """
    results = {}
    for sentiment in ["positive", "negative", "neutral"]:
        subset = breakdown_df[breakdown_df["AI Sentiment"] == sentiment]
        if not subset.empty:
            max_row = subset.loc[subset["percentage"].idxmax()]
            results[sentiment] = {
                "class": max_row[class_col],
                "percentage": max_row["percentage"]
            }
    return results

def plot_sentiment_breakdown(
    breakdown_df: pd.DataFrame,
    class_col: str = "Class Name",
    save_dir: str = "plots",
    show: bool = False
    ):
    
    """
    Plot stacked bar chart of sentiment percentages per clothing class.
    Saves the plot to a file instead of showing in terminal.
    """

    os.makedirs(save_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sentiment_breakdown_{timestamp}.png"
    filepath = os.path.join(save_dir, filename)

    # Build pivot table
    pivot_df = (
        breakdown_df
        .pivot(index=class_col, columns="AI Sentiment", values="percentage")
        .fillna(0)
    )

    # Plot
    plt.figure(figsize=(10, 6))
    pivot_df.plot(kind="bar", stacked=True)

    plt.ylabel("Percentage (%)")
    plt.title("Sentiment Breakdown per Clothing Class")
    plt.legend(title="Sentiment")
    plt.tight_layout()
    plt.savefig(filepath, dpi=300)
    plt.close()

    logger.info(f"Sentiment breakdown plot saved to: {filepath}")
    
    pivot_df = breakdown_df.pivot(index=class_col, columns="AI Sentiment", values="percentage").fillna(0)
    pivot_df.plot(kind="bar", stacked=True, figsize=(10,6))
    plt.ylabel("Percentage (%)")
    plt.title("Sentiment Breakdown per Clothing Class")
    plt.legend(title="Sentiment")
    plt.tight_layout()
    plt.show()

def sentence_case_normalize(t):
    sentences = re.split(r'(?<=[.!?])\s+', t)
    sentences = [s.capitalize() for s in sentences if s]
    return " ".join(sentences)

def preprocess_review(text):
    if not isinstance(text, str) or text.strip() == "":
        return ""

    # normalize unicode
    text = unicodedata.normalize("NFKC", text)
    # collapse whitespace & newlines
    text = re.sub(r"\s+", " ", text).strip()
    # collapse repeated punctuation
    text = re.sub(r"([!?.,])\1+", r"\1", text)
    # sentence capitalization
    text = sentence_case_normalize(text)
    # trim to max length
    if len(text) > 600:
        text = text[:600].rsplit(" ", 1)[0]

    return text

def safe_json_parse(text: str):
    """
    Strict JSON parser.
    Returns None if parsing fails.
    """
    try:
        return json.loads(text)
    except:
        return None

system_prompt = {
    "role": "system",
    "content":"""
    You are a sentiment analysis model. For the given text, return a JSON object with:
    - "sentiment": one of ["positive", "negative", or "neutral"
    - "summary": a one-sentence summary of the text

    No explanations, output only the JSON object.
    """
}

def analyzer(text, client, model="openai/gpt-oss-20b", max_retries=3):
    if not text or not text.strip():
        return "None", "No content provided."
    
    messages = [
        system_prompt,
        {"role": "user", "content": f"Text: {text}"}
    ]
    for attempt in range(1, max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.0,
                max_tokens=150
            )
            raw_output = response.choices[0].message.content.strip()
            print(f"Raw output: {raw_output}")
            parsed = safe_json_parse(raw_output)
            if parsed and "sentiment" in parsed and "summary" in parsed:
                return parsed["sentiment"], parsed["summary"]
            logger.info(f"{attempt}: {raw_output}")
        except Exception as e:
            logger.info(f"API Error on attempt {attempt}: {e}")
        time.sleep(2 ** attempt)
        
    # If all retries fail → return error classification
    return "error", "Model failed to return valid JSON."


if __name__ == "__main__":
    test_text = "I absolutely love this product! It has changed my life for the better."
    sentiment, summary = analyzer(test_text, groq_client)
    print(f"Sentiment: {sentiment}\nSummary: {summary}")