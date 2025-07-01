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

TSB_OUTPUT_COLUMNS = [
    "Date",
    "Transaction Type",
    "Money In",
    "Money Out",
    "Bank Name",
    "Statement Month",
    "Description",
]


def extract_text_with_ocr(pdf_path, page):
    try:
        img = page.to_image(resolution=300).original.convert(
            "L"
        )  # Reduced resolution for speed
        temp_img = "temp_page.png"
        img.save(temp_img)
        text = pytesseract.image_to_string(Image.open(temp_img), config="--psm 6")
        os.remove(temp_img)
        logging.info(f"OCR text extracted for page {page.page_number}:\n{text}")
        return text
    except Exception as e:
        logging.error(
            f"OCR extraction failed for {pdf_path}, page {page.page_number}: {e}"
        )
        return ""


def extract_tsb_transactions(pdf_path):
    transactions = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Log table extraction attempt
                tables = page.extract_tables()
                logging.info(f"Extracted tables on page {page.page_number}: {tables}")

                # Try table extraction
                for table in tables:
                    if any("Date" in cell for cell in table[0] if cell):
                        table = table[1:]  # Skip header
                    for row in table:
                        if len(row) >= 5:
                            date_str = row[0].strip() if row[0] else ""
                            payment_type = row[1].strip() if row[1] else ""
                            description = row[2].strip() if row[2] else ""
                            money_out = row[3].strip() if row[3] else ""
                            money_in = row[4].strip() if row[4] else ""

                            if not re.match(r"\d{2} \w{3} \d{2}", date_str):
                                continue

                            try:
                                money_out = (
                                    float(money_out.replace(",", ""))
                                    if money_out
                                    else 0.0
                                )
                            except (ValueError, TypeError):
                                money_out = 0.0
                            try:
                                money_in = (
                                    float(money_in.replace(",", ""))
                                    if money_in
                                    else 0.0
                                )
                            except (ValueError, TypeError):
                                money_in = 0.0

                            full_description = f"{payment_type} {description}".strip()
                            transactions.append(
                                {
                                    "date": date_str,
                                    "description": full_description,
                                    "money_out": money_out,
                                    "money_in": money_in,
                                }
                            )

                # Fallback to OCR-based text extraction
                if not transactions:  # Only use OCR if table extraction failed
                    text = extract_text_with_ocr(pdf_path, page)
                    if text:
                        lines = text.split("\n")
                        for line in lines:
                            line = line.strip()
                            if (
                                not line
                                or "Your Transactions" in line
                                or "Date Payment type Details" in line
                                or "Page" in line
                                or "PDF Redactor Free" in line
                            ):
                                continue
                            # Match TSB transaction line format: "DD MMM YY TYPE DESCRIPTION [AMOUNT] [AMOUNT]"
                            match = re.match(
                                r"^(\d{2} \w{3} \d{2})\s+([A-Z\s/]+)\s+(.+?)\s+([\d,]+\.\d{2,4})?\s*([\d,]+\.\d{2,4})?$",
                                line,
                            )
                            if match:
                                date_str = match.group(1)
                                payment_type = match.group(2).strip()
                                description = match.group(3).strip()
                                money_out = match.group(4) if match.group(4) else ""
                                money_in = match.group(5) if match.group(5) else ""

                                try:
                                    money_out = (
                                        float(money_out.replace(",", ""))
                                        if money_out
                                        else 0.0
                                    )
                                except (ValueError, TypeError):
                                    money_out = 0.0
                                try:
                                    money_in = (
                                        float(money_in.replace(",", ""))
                                        if money_in
                                        else 0.0
                                    )
                                except (ValueError, TypeError):
                                    money_in = 0.0

                                full_description = (
                                    f"{payment_type} {description}".strip()
                                )
                                transactions.append(
                                    {
                                        "date": date_str,
                                        "description": full_description,
                                        "money_out": money_out,
                                        "money_in": money_in,
                                    }
                                )
    except Exception as e:
        logging.error(f"Failed to extract TSB transactions: {e}")
    return transactions


def normalize_tsb(transactions, pdf_name):
    normalized = []
    statement_month = "July 2024"  # Default based on PDF content
    if transactions:
        try:
            first_date = datetime.strptime(transactions[0]["date"], "%d %b %y")
            statement_month = first_date.strftime("%b %Y")
        except (ValueError, KeyError):
            pass

    for row in transactions:
        try:
            date_str = row.get("date")
            date_obj = datetime.strptime(date_str, "%d %b %y")
            date = date_obj.strftime("%Y-%m-%d")

            money_in = float(row.get("money_in", 0.0))
            money_out = float(row.get("money_out", 0.0))
            trans_type = (
                "Received" if money_in > 0 else "Payment" if money_out > 0 else "Other"
            )

            normalized.append(
                {
                    "Date": date,
                    "Transaction Type": trans_type,
                    "Money In": money_in,
                    "Money Out": money_out,
                    "Bank Name": "TSB",
                    "Statement Month": statement_month,
                    "Description": row.get("description", "").strip(),
                }
            )
        except Exception as e:
            logging.debug(f"Failed to normalize TSB row: {row} Error: {e}")
    return normalized


def extract_tsb_redacted_to_excel(input_pdf, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)
    pdf_name = os.path.basename(input_pdf)
    logging.info(f"Processing {pdf_name}")

    raw_data = extract_tsb_transactions(input_pdf)
    logging.info(f"Raw transactions extracted: {len(raw_data)}")
    normalized = normalize_tsb(raw_data, pdf_name)

    if normalized:
        df = pd.DataFrame(normalized, columns=TSB_OUTPUT_COLUMNS)
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
        output_path = os.path.join(output_dir, "tsb_redacted_001.xlsx")
        df.to_excel(output_path, index=False, engine="openpyxl")
        logging.info(f"Saved {len(df)} transactions to {output_path}")
    else:
        logging.warning("No TSB transactions extracted.")


if __name__ == "__main__":
    extract_tsb_redacted_to_excel("bank_statements/TSB Redacted 2.pdf")
