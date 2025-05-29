import pandas as pd
from datetime import datetime
import logging
import argparse
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def generate_new_filename(base, ext, suffix="_validated"):
    """Generate a new filename if one already exists to avoid overwriting."""
    attempt = 1
    new_filename = f"{base}{suffix}{ext}"
    while os.path.exists(new_filename):
        new_filename = f"{base}{suffix}_{attempt}{ext}"
        attempt += 1
    return new_filename

def validate_and_filter_csv(file_path, start_date, end_date):
    """Load CSV and filter out companies not created within the given date range."""
    try:
        logging.info(f"Loading file: {file_path}")
        df = pd.read_csv(file_path)

        if "date_of_creation" not in df.columns:
            logging.error("Missing 'date_of_creation' column in CSV.")
            return None

        df["date_of_creation"] = pd.to_datetime(df["date_of_creation"], errors="coerce")
        original_count = len(df)

        # Filter by date range
        df = df[(df["date_of_creation"] >= start_date) & (df["date_of_creation"] <= end_date)]
        filtered_count = len(df)

        logging.info(f"Filtered out {original_count - filtered_count} entries outside the date range.")
        
        # Prepare safe output filename
        base, ext = os.path.splitext(file_path)
        output_file = generate_new_filename(base, ext)

        df.to_csv(output_file, index=False)
        logging.info(f"Validated data saved to: {output_file}")
        return output_file

    except Exception as e:
        logging.error(f"Error processing file: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate and filter companies by incorporation date.")
    parser.add_argument("csv_file", nargs="?", help="Path to the CSV file to validate")
    parser.add_argument("--start_date", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", help="End date in YYYY-MM-DD format")

    args = parser.parse_args()

    if not args.csv_file or not args.start_date or not args.end_date:
        logging.error("Usage: python3 filter_and_validate.py <csv_file> --start_date YYYY-MM-DD --end_date YYYY-MM-DD")
        exit(1)

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        logging.error("Invalid date format. Use YYYY-MM-DD.")
        exit(1)

    validate_and_filter_csv(args.csv_file, start_date, end_date)
