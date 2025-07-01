import os
import re
import logging
from datetime import datetime
from pathlib import Path
import pdfplumber
import pandas as pd
import pytesseract
from PIL import Image

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("convert_statements.log"), logging.StreamHandler()],
)

logging.getLogger("pdfplumber").setLevel(logging.ERROR)

# Define bank-specific configurations
BANK_CONFIGS = {
    "Barclays": {
        "name_pattern": r"Barclays|Barclays Bank",
        "file_pattern": r"barclays",
        "date_regex": r"^\d{2}\s[A-Za-z]{3}",
        "date_format": "%d %b",
        "columns": ["Date", "Description", "Money Out", "Money In", "Balance"],
        "money_in_col": "Money In",
        "money_out_col": "Money Out",
        "transaction_type_col": "Description",
    },
}

OUTPUT_COLUMNS = [
    "Date",
    "Transaction Type",
    "Money In",
    "Money Out",
    "Bank Name",
    "Statement Month",
    "Description",
]


def setup_directories(input_dir="bank_statements", output_dir="output"):
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Input directory: {input_dir}, Output directory: {output_dir}")


def identify_bank(pdf_path, filename):
    for bank, config in BANK_CONFIGS.items():
        if re.search(config["file_pattern"], filename, re.IGNORECASE):
            return bank
    try:
        with pdfplumber.open(pdf_path) as pdf:
            first_page_text = pdf.pages[0].extract_text() or ""
            for bank, config in BANK_CONFIGS.items():
                if re.search(config["name_pattern"], first_page_text, re.IGNORECASE):
                    return bank
    except Exception:
        pass
    return "Unknown"


def extract_text_with_ocr(pdf_path, page):
    try:
        img = page.to_image(resolution=600).original.convert("L")
        temp_img = "temp_page.png"
        img.save(temp_img)
        text = pytesseract.image_to_string(Image.open(temp_img), config="--psm 6")
        os.remove(temp_img)
        return text
    except Exception as e:
        logging.error(f"OCR extraction failed for {pdf_path}: {e}")
        return ""


def extract_barclays_transactions(pdf_path):
    transactions = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    text = extract_text_with_ocr(pdf_path, page)
                if not text:
                    continue
                lines = text.split("\n")
                current = None
                for line in lines:
                    line = line.strip()
                    # Skip headers or irrelevant lines
                    if (
                        not line
                        or "Your transactions" in line
                        or "Date Description" in line
                        or "Page" in line
                        or "Barclays Bank UK PLC" in line
                    ):
                        continue
                    # Start of a new transaction (date pattern: "DD Mon")
                    if re.match(r"^\d{2}\s[A-Za-z]{3}", line):
                        if current:
                            transactions.append(current)
                        current = {
                            "date": line[:6],
                            "description": "",
                            "money_out": "",
                            "money_in": "",
                            "balance": "",
                        }
                    # Process transaction details
                    elif current:
                        # Extract amounts (e.g., 1,234.56 or 12.34)
                        amounts = re.findall(r"[\d,]+\.\d{2}|\d+\.\d{2}", line)
                        # Clean description by removing amounts
                        desc = re.sub(r"[\d,]+\.\d{2}|\d+\.\d{2}", "", line).strip()
                        # Remove date prefix from description if present
                        desc = re.sub(r"^\d{2}\s[A-Za-z]{3}\s", "", desc).strip()
                        if "Received From" in desc or "Giro Received" in desc:
                            # Money In transaction
                            if amounts:
                                current["money_in"] = amounts[0].replace(",", "")
                                if len(amounts) > 1:
                                    current["balance"] = amounts[-1].replace(",", "")
                            current["description"] += " " + desc
                        else:
                            # Money Out transaction
                            if amounts:
                                current["money_out"] = amounts[0].replace(",", "")
                                if len(amounts) > 1:
                                    current["balance"] = amounts[-1].replace(",", "")
                            current["description"] += " " + desc
                # Append the last transaction
                if current:
                    current["description"] = current["description"].strip()
                    transactions.append(current)
    except Exception as e:
        logging.error(f"Failed to extract Barclays transactions: {e}")
    return transactions


def normalize_barclays(transactions, pdf_name):
    normalized = []
    statement_month = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s?\d{4}",
        pdf_name,
        re.IGNORECASE,
    )
    statement_month = statement_month.group(0) if statement_month else "April 2025"
    for row in transactions:
        try:
            date_str = row.get("date")
            date_obj = datetime.strptime(date_str, "%d %b")
            date = date_obj.replace(year=2025).strftime("%Y-%m-%d")
            description = row.get("description", "").strip()
            money_in = (
                float(re.sub(r"[^\d.]", "", row.get("money_in", "0")))
                if row.get("money_in")
                else 0.0
            )
            money_out = (
                float(re.sub(r"[^\d.]", "", row.get("money_out", "0")))
                if row.get("money_out")
                else 0.0
            )
            trans_type = "Received" if money_in > 0 else "Payment"

            normalized.append(
                {
                    "Date": date,
                    "Transaction Type": trans_type,
                    "Money In": money_in,
                    "Money Out": money_out,
                    "Bank Name": "Barclays",
                    "Statement Month": statement_month,
                    "Description": description,
                }
            )
        except Exception as e:
            logging.debug(f"Failed to normalize Barclays row: {row} Error: {e}")
    return normalized


def process_statements(input_dir="bank_statements", output_dir="output"):
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
        else:
            logging.warning(f"No handler yet for bank: {bank_name}")
            normalized = []

        all_transactions.extend(normalized)
        logging.info(f"Extracted {len(normalized)} transactions from {pdf_file}")

    if all_transactions:
        df = pd.DataFrame(all_transactions, columns=OUTPUT_COLUMNS)
        df.sort_values(by="Date", inplace=True)
        df.drop_duplicates(
            subset=[
                "Date",
                "Transaction Type",
                "Money In",
                "Money Out",
                "Bank Name",
                "Description",
            ],
            inplace=True,
        )
        output_path = os.path.join(
            output_dir,
            f"barclay_bank_extracted_statement_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
        )
        df.to_excel(output_path, index=False, engine="openpyxl")
        logging.info(f"Saved {len(df)} transactions to {output_path}")
    else:
        logging.warning("No transactions to save.")


def cleanup_temp_files():
    temp_files = [f for f in os.listdir() if f.startswith("temp_")]
    for f in temp_files:
        try:
            os.remove(f)
        except Exception as e:
            logging.error(f"Failed to delete {f}: {e}")


def main():
    try:
        if os.path.exists("debug_extraction.txt"):
            os.remove("debug_extraction.txt")
        process_statements()
    finally:
        cleanup_temp_files()


if __name__ == "__main__":
    main()
