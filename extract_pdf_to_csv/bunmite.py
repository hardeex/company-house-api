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
    handlers=[logging.FileHandler("virginmoney_extract.log"), logging.StreamHandler()],
)
logging.getLogger("pdfplumber").setLevel(logging.ERROR)

BANK_CONFIG = {
    "Virgin Money": {
        "name_pattern": r"Virgin Money|Clydesdale Bank",
        "file_pattern": r"bunmite",
        "date_regex": r"^\d{2}\s[A-Za-z]{3}",
        "date_format": "%d %b",
        "columns": ["Date", "Description", "Debits", "Credits", "Balance"],
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
        return text
    except Exception as e:
        logging.error(f"OCR extraction failed for {pdf_path}: {e}")
        return ""

def extract_virginmoney_transactions(pdf_path: str) -> List[Dict[str, str]]:
    transactions: List[Dict[str, str]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    text = extract_text_with_ocr(pdf_path, page)
                if not text:
                    continue
                lines = text.split("\n")
                current: Dict[str, str] = {}
                for line in lines:
                    line = line.strip()
                    if not line or "Previous statement" in line or "Balance brought forward" in line or "Page" in line or "Virgin Money" in line or "Statement No" in line:
                        continue
                    if re.match(BANK_CONFIG["Virgin Money"]["date_regex"], line):
                        if current:
                            transactions.append(current)
                        current = {"date": line[:6], "description": "", "money_out": "", "money_in": "", "balance": ""}
                    elif current:
                        amounts = re.findall(r"[\d,]+\.\d{2}|\d+\.\d{2}", line)
                        desc = re.sub(r"[\d,]+\.\d{2}|\d+\.\d{2}", "", line).strip()
                        desc = re.sub(r"^\d{2}\s[A-Za-z]{3}\s", "", desc).strip()
                        if amounts:
                            if len(amounts) >= 2:
                                current["money_out"] = amounts[0].replace(",", "")
                                current["money_in"] = amounts[1].replace(",", "")
                                if len(amounts) > 2:
                                    current["balance"] = amounts[-1].replace(",", "")
                            elif "Card" in desc or "Mb" in desc.lower():
                                current["money_out"] = amounts[0].replace(",", "")
                            else:
                                current["money_in"] = amounts[0].replace(",", "")
                            if len(amounts) == 2:
                                current["balance"] = amounts[-1].replace(",", "")
                        current["description"] = (current["description"] + " " + desc).strip()
                if current:
                    transactions.append(current)
    except Exception as e:
        logging.error(f"Failed to extract Virgin Money transactions: {e}")
    return transactions

def normalize_virginmoney(transactions: List[Dict[str, str]], pdf_name: str) -> List[Dict[str, any]]:
    normalized = []
    statement_month = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s?\d{4}",
        pdf_name,
        re.IGNORECASE,
    )
    statement_month = statement_month.group(0) if statement_month else "Unknown"
    for row in transactions:
        try:
            date_str = row.get("date", "")
            if not date_str:
                continue
            date_obj = datetime.strptime(date_str, BANK_CONFIG["Virgin Money"]["date_format"])
            date = date_obj.replace(year=2024).strftime("%Y-%m-%d")
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
            trans_type = "Received" if money_in > 0 else "Payment"
            normalized.append(
                {
                    "Date": date,
                    "Transaction Type": trans_type,
                    "Money In": money_in,
                    "Money Out": money_out,
                    "Bank Name": "Virgin Money",
                    "Statement Month": statement_month,
                    "Description": description,
                    "Balance": balance,
                }
            )
        except Exception as e:
            logging.debug(f"Failed to normalize Virgin Money row: {row} Error: {e}")
    return normalized