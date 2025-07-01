import os
import re
import logging
from datetime import datetime
from typing import List, Dict
import pdfplumber
import pandas as pd
import pytesseract
from PIL import Image

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("nationwide_extract.log"), logging.StreamHandler()],
)
logging.getLogger("pdfplumber").setLevel(logging.ERROR)

BANK_CONFIG = {
    "Nationwide": {
        "name_pattern": r"Nationwide",
        "file_pattern": r"nationwide",
        "date_regex": r"^\d{2}\s[A-Za-z]{3}",
        "date_format": "%d %b",
        "columns": ["Date", "Description", "E Out", "E In", "E Balance"],
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
    "Balance",
]

def extract_text_with_ocr(pdf_path: str, page) -> str:
    try:
        img = page.to_image(resolution=600).original.convert("L")
        temp_img = "temp_page.png"
        img.save(temp_img)
        text = pytesseract.image_to_string(Image.open(temp_img), config="--psm 6")
        os.remove(temp_img)
        logging.debug(f"OCR extracted text for {pdf_path}: {text[:100]}...")
        return text
    except Exception as e:
        logging.error(f"OCR extraction failed for {pdf_path}: {e}")
        return ""

def extract_nationwide_transactions(pdf_path: str) -> List[Dict[str, str]]:
    transactions: List[Dict[str, str]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    text = extract_text_with_ocr(pdf_path, page)
                if not text:
                    logging.warning(f"No text extracted from page in {pdf_path}")
                    continue
                logging.debug(f"Extracted text from {pdf_path}: {text[:100]}...")
                lines = text.split("\n")
                current: Dict[str, str] = {}
                for line in lines:
                    line = line.strip()
                    if not line or "Balance from statement" in line or "Your FlexPlus" in line or "PDF-Redacter-Free" in line or "Please check your statement" in line or "Interest, Rates and Fees" in line:
                        continue
                    if re.match(BANK_CONFIG["Nationwide"]["date_regex"], line):
                        if current:
                            transactions.append(current)
                        current = {"date": line[:6], "description": "", "money_out": "", "money_in": "", "balance": ""}
                        logging.debug(f"New transaction started with date: {line[:6]}")
                    elif current:
                        amounts = re.findall(r"[\d,]+\.\d{2}|\d+\.\d{2}", line)
                        desc = re.sub(r"[\d,]+\.\d{2}|\d+\.\d{2}", "", line).strip()
                        desc = re.sub(r"^\d{2}\s[A-Za-z]{3}\s", "", desc).strip()
                        if "Effective Date" in desc:
                            continue
                        if amounts:
                            if len(amounts) >= 2:
                                current["money_out"] = amounts[0].replace(",", "")
                                current["money_in"] = amounts[1].replace(",", "")
                                if len(amounts) > 2:
                                    current["balance"] = amounts[-1].replace(",", "")
                            elif "Bank credit" in desc or "Cash credit" in desc or "Transfer from" in desc:
                                current["money_in"] = amounts[0].replace(",", "")
                            else:
                                current["money_out"] = amounts[0].replace(",", "")
                            if len(amounts) == 1 and re.search(r"\d+\.\d{2}\s*$", line):
                                current["balance"] = amounts[-1].replace(",", "")
                        current["description"] = (current["description"] + " " + desc).strip()
                        logging.debug(f"Added to transaction: desc={desc}, amounts={amounts}")
                if current:
                    transactions.append(current)
    except Exception as e:
        logging.error(f"Failed to extract Nationwide transactions from {pdf_path}: {e}")
    return transactions

def normalize_nationwide(transactions: List[Dict[str, str]], pdf_name: str) -> List[Dict[str, any]]:
    normalized = []
    statement_month = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s?\d{4}",
        pdf_name,
        re.IGNORECASE,
    )
    statement_month = statement_month.group(0) if statement_month else "Unknown"
    year = 2025 if "2025" in pdf_name else 2024
    for row in transactions:
        try:
            date_str = row.get("date", "")
            if not date_str:
                continue
            date_obj = datetime.strptime(date_str, BANK_CONFIG["Nationwide"]["date_format"])
            date = date_obj.replace(year=year).strftime("%Y-%m-%d")
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
            balance = (
                float(re.sub(r"[^\d.]", "", row.get("balance", "0")))
                if row.get("balance")
                else 0.0
            )
            trans_type = (
                "Received" if money_in > 0
                else "Payment" if money_out > 0
                else "Unknown"
            )
            if "Bank credit" in description or "Cash credit" in description:
                trans_type = "Credit"
            elif "Transfer to" in description:
                trans_type = "Transfer Out"
            elif "Transfer from" in description:
                trans_type = "Transfer In"
            elif "Direct debit" in description:
                trans_type = "Direct Debit"
            elif "Monthly Account Fee" in description:
                trans_type = "Fee"
            normalized.append(
                {
                    "Date": date,
                    "Transaction Type": trans_type,
                    "Money In": money_in,
                    "Money Out": money_out,
                    "Bank Name": "Nationwide",
                    "Statement Month": statement_month,
                    "Description": description,
                    "Balance": balance,
                }
            )
            logging.debug(f"Normalized transaction: {date}, {trans_type}, {money_in}, {money_out}, {description}")
        except Exception as e:
            logging.debug(f"Failed to normalize Nationwide row: {row} Error: {e}")
    return normalized

def setup_directories(input_dir: str = "bank_statements", output_dir: str = "output") -> None:
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Input directory: {input_dir}, Output directory: {output_dir}")

def process_statements(input_dir: str = "bank_statements", output_dir: str = "output") -> None:
    setup_directories(input_dir, output_dir)
    all_transactions = []

    for pdf_file in sorted(os.listdir(input_dir)):
        if not pdf_file.lower().endswith(".pdf") or not re.search(BANK_CONFIG["Nationwide"]["file_pattern"], pdf_file, re.IGNORECASE):
            logging.info(f"Skipping file: {pdf_file}")
            continue
        pdf_path = os.path.join(input_dir, pdf_file)
        logging.info(f"Processing {pdf_file}")

        raw_data = extract_nationwide_transactions(pdf_path)
        normalized = normalize_nationwide(raw_data, pdf_file)
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
            f"nationwide_statements_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
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