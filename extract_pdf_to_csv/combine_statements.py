import os
import re
import logging
from datetime import datetime
import pdfplumber
import pandas as pd
from barclays_extract import extract_barclays_transactions, normalize_barclays
from virginmoney_extract import extract_virginmoney_transactions, normalize_virginmoney
from nationwide_extract import extract_nationwide_transactions, normalize_nationwide
from tsb_extract import extract_tsb_transactions, normalize_tsb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("combine_statements.log"), logging.StreamHandler()],
)

OUTPUT_COLUMNS = [
    "Date",
    "Transaction Type",
    "Money In",
    "Money Out",
    "Bank Name",
    "Statement Month",
    "Description",
    "Balance",
]

def setup_directories(input_dir: str = "bank_statements", output_dir: str = "output") -> None:
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Input directory: {input_dir}, Output directory: {output_dir}")

def identify_bank(pdf_path: str, filename: str) -> str:
    bank_patterns = {
        "Barclays": r"barclays",
        "Virgin Money": r"bunmite|Virgin Money|Clydesdale Bank",
        "Nationwide": r"nationwide",
        "TSB": r"tsb|PSB",
    }
    for bank, pattern in bank_patterns.items():
        if re.search(pattern, filename, re.IGNORECASE):
            return bank
    try:
        with pdfplumber.open(pdf_path) as pdf:
            first_page_text = pdf.pages[0].extract_text() or ""
            for bank, pattern in bank_patterns.items():
                if re.search(pattern, first_page_text, re.IGNORECASE):
                    return bank
    except Exception:
        pass
    return "Unknown"

def process_statements(input_dir: str = "bank_statements", output_dir: str = "output") -> None:
    setup_directories(input_dir, output_dir)
    all_transactions = []

    for pdf_file in sorted(os.listdir(input_dir)):
        if not pdf_file.lower().endswith(".pdf"):
            continue
        pdf_path = os.path.join(input_dir, pdf_file)
        logging.info(f"Processing {pdf_file}")

        bank_name = identify_bank(pdf_path, pdf_file)
        logging.info(f"Detected bank: {bank_name} for {pdf_file}")

        if bank_name == "Barclays":
            raw_data = extract_barclays_transactions(pdf_path)
            normalized = normalize_barclays(raw_data, pdf_file)
        elif bank_name == "Virgin Money":
            raw_data = extract_virginmoney_transactions(pdf_path)
            normalized = normalize_virginmoney(raw_data, pdf_file)
        elif bank_name == "Nationwide":
            raw_data = extract_nationwide_transactions(pdf_path)
            normalized = normalize_nationwide(raw_data, pdf_file)
        elif bank_name == "TSB":
            raw_data = extract_tsb_transactions(pdf_path)
            normalized = normalize_tsb(raw_data, pdf_file)
        else:
            logging.warning(f"No handler for bank: {bank_name}")
            normalized = []

        all_transactions.extend(normalized)
        logging.info(f"Extracted {len(normalized)} transactions from {pdf_file}")

    if all_transactions:
        df = pd.DataFrame(all_transactions, columns=OUTPUT_COLUMNS)
        df.sort_values(by="Date", inplace=True)
        df.drop_duplicates(
            subset=["Date", "Transaction Type", "Money In", "Money Out", "Bank Name", "Description"],
            inplace=True,
        )
        output_path = os.path.join(
            output_dir,
            f"consolidated_statements_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
        )
        df.to_excel(output_path, index=False, engine="openpyxl")
        logging.info(f"Saved {len(df)} transactions to {output_path}")
    else:
        logging.warning("No transactions to save.")

def cleanup_temp_files() -> None:
    temp_files = [f for f in os.listdir() if f.startswith("temp_")]
    for f in temp_files:
        try:
            os.remove(f)
        except Exception as e:
            logging.error(f"Failed to delete {f}: {e}")

def main() -> None:
    try:
        if os.path.exists("debug_extraction.txt"):
            os.remove("debug_extraction.txt")
        process_statements()
    finally:
        cleanup_temp_files()

if __name__ == "__main__":
    main()