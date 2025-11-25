
# Automated Review Analysis Pipeline

## Overview
This project builds an automated pipeline to:
- Load raw customer review data into Google Sheets
- Clean and stage the data
- Process reviews with AI sentiment classification and summaries
- Write results back into Google Sheets

## Features
- Modular ETL pipeline
- Idempotent worksheet creation
- Integration with Groq LLM (openai/gpt-oss-20b)
- Actionable insights from customer reviews

## Setup
1. Clone repo
2. Install dependencies: `pip install -r requirements.txt`
3. Add your Google service account JSON path in `.env`
4. Run pipeline: `python main.py`

## Structure
- `src/utils.py` → credentials & worksheet helpers
- `src/etl.py` → extract-transform-load logic
- `src/analysis.py` → AI sentiment & summaries
- `tests/` → unit tests
