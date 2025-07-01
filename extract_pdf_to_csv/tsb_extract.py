import os
import re
import logging
from datetime import datetime
from typing import List, Dict
import pdfplumber
import pandas as pd
import pytesseract
from PIL import Image, ImageEnhance

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("tsb_extract_april.log"), logging.StreamHandler()],
)
logging.getLogger("pdfplumber").setLevel(logging.ERROR)

BANK_CONFIG = {
    "TSB": {
        "name_pattern": r"TSB|PSB",
        "file_pattern": r"TSB_April_2025",
        "date_regex": r"^\d{2}\s+[A-Za-z]{3}\s+\d{2}\b",
        "date_format": "%d %b %y",
        "columns": [
            "Date",
            "Payment type",
            "Details",
            "Money Out (£)",
            "Money In (£)",
            "Balance (£)",
        ],
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

# Provided OCR content for TSB_April_2025.pdf
PROVIDED_OCR = {
    1: """PDF Redactor Free
PSB
Fees Explained
Other services - These are fees for other services you have asked for. You can find more details in our Banking Charges guide or at www.tsb.co.uk The month you can use the www. pssb.co.uk. The Classic Plus Account is the Classic Plus Account is £30. Further details can be found on the 150.00 .00 .00 .00 .00 .00 .00$. The Frees and interest rates may have changed during the period covered by this summary. For details please see your regular statements.
Page 1 of 7""",
    2: "",
    3: """PDF Redactor Free
Your Transactions

| Date | Payment type | Details | Money Out (£) | Money In (£) | Balance (£) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 03 Mar 24 |  | STATEMENT OPENING BALANCE |  |  | 319.82 OD |
| 04 Mar 24 |  | LEMONADE FINANCE CD 3947 |  |  | 419.82 OD |
| 04 Mar 24 | FASTER PAYMENT | debt | 100.00 |  | 519.82 OD |
| 04 Mar 24 | FASTER PAYMENT |  | 60.00 |  | 579.82 OD |
| 04 Mar 24 | CASH DEPOSIT |  |  | 580.00 | 0.18 |
| 05 Mar 24 |  | LONDON GB <br> LA LOUNGE CD 3947 |  |  | 269.82 OD |
| 05 Mar 24 | FASTER PAYMENT | Baba Gambo <br> USMAN |  | 100.00 | 169.82 OD |
| 06 Mar 24 | DIRECT DEBIT | H3G REFERENCE: <br> 971948539401030730 |  |  | 188.98 OD |
| 06 Mar 24 |  | CO-OP GROUP 070815 CD 3947 |  |  | 191.21 OD |
| 07 Mar 24 |  | MR PRETZELS (UK) RETAI CD 3947 |  |  | 207.01 OD |
| 08 Mar 24 |  | LEMONADE FINANCE CD 3947 |  |  | 407.01 OD |
| 08 Mar 24 | FASTER PAYMENT |  |  |  | 457.01 OD |
| 09 Mar 24 | FASTER PAYMENT |  |  |  | 397.01 OD |
| 09 Mar 24 | CASH DEPOSIT | PO 3 AIRE DRIVE <br> OCKENDO GB |  |  | 47.01 OD |
| 09 Mar 24 | FASTER PAYMENT |  |  |  | 77.01 OD |
| 11 Mar 24 | DIRECT DEBIT |  |  |  | 86.17 OD |
| 11 Mar 24 | FASTER PAYMENT |  |  |  | 136.17 OD |
| 11 Mar 24 | FASTER PAYMENT |  |  |  | 286.17 OD |
| 12 Mar 24 | FASTER PAYMENT |  |  |  | 548.91 |
| 12 Mar 24 | FASTER PAYMENT |  |  |  | 515.92 |
| 12 Mar 24 | FASTER PAYMENT |  |  |  | 385.92 |
| 12 Mar 24 | FASTER PAYMENT |  |  |  | 114.08 OD |
| 14 Mar 24 | CASH DEPOSIT |  |  |  | 85.92 |
| 14 Mar 24 | FASTER PAYMENT |  |  |  | 585.92 |
| 14 Mar 24 | FASTER PAYMENT |  |  |  | 85.92 |
| 15 Mar 24 |  |  |  |  | 10.65 OD |

Continued on next page
Page 3 of 7""",
    4: """PDF Redactor Free

Statement number: 76
Classic Plus Account

Your Transactions

| Date | Payment type | Details | Money Out (£) | Money In (£) | Balance (£) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 15 Mar 24 | FASTER PAYMENT | KURAMOH LOUN <br> Fice |  | 70.00 | 40.6500 |
| 16 Mar 24 | FASTER PAYMENT | Nala Payments Limited banked <br> BB234969 | 100.00 |  | 140.6500 |
| 16 Mar 24 | FASTER PAYMENT | Nala Payments Limited banked <br> BB234969 | 300.00 |  | 440.6500 |
| 16 Mar 24 | FASTER PAYMENT | Nala Payments Limited banked <br> BB234969 | 40.00 |  | 480.6500 |
| 18 Mar 24 |  | LUL GB STH OCKENDON CD 3947 | 16.95 |  | 497.6000 |
| 18 Mar 24 |  | LUL GB STH OCKENDON CD 3947 | 4.18 |  | 501.7800 |
| 18 Mar 24 |  | 4 ANGELS FOODS CD 3947 | 21.97 |  | 523.7500 |
| 18 Mar 24 |  | ANGEL?S BAKERY CD 3947 | 2.60 |  | 526.3500 |
| 18 Mar 24 | FASTER PAYMENT |  | 215.00 |  | 741.3500 |
| 18 Mar 24 | TRANSFER | FROM Easy Saver 774927-01937268 |  | 880.00 | 138.65 |
| 18 Mar 24 | FASTER PAYMENT | Nala Payments Limited banked <br> BB234969 | 200.00 |  | 61.3500 |
| 19 Mar 24 | FASTER PAYMENT |  | 75.00 |  | 136.3500 |
| 19 Mar 24 | FASTER PAYMENT |  | 40.00 |  | 176.3500 |
| 19 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 19 Mar 24 | FASTER PAYMENT |  | 200.00 |  | 376.3500 |
| 20 Mar 24 | FASTER PAYMENT | Nala Payments Limited banked <br> BB234969 | 300.00 |  | 874.3500 |
| 20 Mar 24 |  | TESCO ROMFORD |  |  |  |
| 20 Mar 24 |  |  |  |  |  |
| 22 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 23 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 25 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 26 Mar 24 | DIRECT CREDIT |  |  |  |  |
| 26 Mar 24 |  |  |  |  |  |
| 26 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 26 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 27 Mar 24 |  |  |  |  |  |
| 27 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 27 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 28 Mar 24 |  |  |  |  |  |
| 28 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 29 Mar 24 | FASTER PAYMENT |  |  |  |  |
| 30 Mar 24 | FASTER PAYMENT |  |  |  |  |

Continued on next page
Page 4 of 7""",
    5: """PDF Redactor Free
Statement number: 76
Classic Plus Account

Your Transactions

| Date | Payment type | Details | Money Out (£) | Money In (£) | Balance (£) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 31 Mar 24 | FASTER PAYMENT | Card ending 2940 <br> 5402225002292940 | 100.00 |  | 870.76 |
| 02 Apr 24 | DIRECT DEBIT | SANTANDER MORTGAGE <br> REFERENCE: 043135951 | 1,599.81 |  | 729.0500 |
| 02 Apr 24 | FASTER PAYMENT | KURAMOH LOUN <br> Ajor |  | 2,000.00 | 1,270.95 |
| 02 Apr 24 | SERVICE CHARGES | DEBIT INTEREST ARRANGED O/D | 8.33 |  | 1,262.62 |
| 02 Apr 24 | FASTER PAYMENT | Temitope Ayeni <br> Clothe | 770.00 |  | 492.62 |
| 02 Apr 24 | FASTER PAYMENT | debt <br> FARA TRAVELS | 140.00 |  | 352.62 |
| 02 Apr 24 | FASTER PAYMENT | Ticket <br> FROM Easy Saver 774927-01937268 |  |  | 347.3800 |
| 02 Apr 24 | TRANSFER | STATEMENT CLOSING BALANCE | 9,448.44 | 9,820.88 | 52.62 |

Page 5 of 7""",
    6: """Notification of forthcoming fees for your next statement period
PDF Redactor Free
Fees for your monthly billing period 02 Mar 24 to 01 Apr 24
Debit interest
Total Arranged fees
Total fees
£8.11
£8.11
£8.11

These fees will be debited at close of business on 02 May 24
Monthly Maximum Charge (MMC) for Overdraft Fees and Interest
To make comparing bank accounts easier for you, all banks and building societies are setting a maximum monthly charge. Each bank may charge a different amount, but all banks are explaining this to their customers in the same way with the same wording. This wording is in the box below.

We'll never charge you more than £30 each monthly billing period for interest charged on the amount you borrow using an Unarranged Overdraft.

Monthly cap on unarranged overdraft charges

1. Each current account will set a monthly maximum charge for:
a. going overdrawn when you have not arranged an overdraft; or
b. going over/past your arranged overdraft limit (if you have one)
2. This cap covers:
a. Interest for going over/past your arranged overdraft limit
b. Fees for each payment your bank allows despite lack of funds; and
c. Fees for each payment your bank refuses due to lack of funds
Page 6 of 7""",
    7: """PDF Redactor Free
PSB""",
    8: """PDF Redactor Free""",
}


def extract_text_with_ocr(pdf_path: str, page, page_num: int) -> str:
    try:
        img = page.to_image(resolution=600).original.convert(
            "L"
        )  # Reduced to avoid DecompressionBomb
        img = ImageEnhance.Contrast(img).enhance(2.0)
        temp_img = f"temp_page_{os.path.basename(pdf_path)}_{page_num}.png"
        img.save(temp_img)
        text = pytesseract.image_to_string(
            Image.open(temp_img), config="--psm 4 --oem 3"
        )
        os.remove(temp_img)
        logging.debug(
            f"OCR extracted text for {pdf_path}, page {page_num}: {text[:200]}..."
        )
        with open(f"debug_ocr_{os.path.basename(pdf_path)}_{page_num}.txt", "w") as f:
            f.write(text)
        return text
    except Exception as e:
        logging.error(f"OCR extraction failed for {pdf_path}, page {page_num}: {e}")
        return ""


def extract_tsb_transactions(
    pdf_path: str, use_provided_ocr: bool = True
) -> List[Dict[str, str]]:
    transactions: List[Dict[str, str]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logging.debug(f"Processing {pdf_path} with {len(pdf.pages)} pages")
            for page_num, page in enumerate(pdf.pages, 1):
                text = (
                    PROVIDED_OCR.get(page_num, "")
                    if use_provided_ocr
                    else page.extract_text()
                )
                if not text:
                    logging.warning(
                        f"No text extracted with pdfplumber from {pdf_path}, page {page_num}. Attempting OCR."
                    )
                    text = extract_text_with_ocr(pdf_path, page, page_num)
                if not text:
                    logging.warning(
                        f"No text extracted from {pdf_path}, page {page_num}"
                    )
                    continue
                logging.debug(
                    f"Extracted text from {pdf_path}, page {page_num}: {text[:200]}..."
                )
                with open(
                    f"debug_text_{os.path.basename(pdf_path)}_{page_num}.txt", "w"
                ) as f:
                    f.write(text)
                lines = text.split("\n")
                current: Dict[str, str] = {}
                for line in lines:
                    line = line.strip()
                    if not line or any(
                        skip in line
                        for skip in [
                            "PDF Redactor Free",
                            "Fees Explained",
                            "Continued on next page",
                            "Statement number",
                            "Classic Plus Account",
                            "Your Transactions",
                            "Page",
                            "Notification of forthcoming fees",
                            "Monthly cap on unarranged overdraft charges",
                            "| Date | Payment type | Details | Money Out (£) | Money In (£) | Balance (£) |",
                        ]
                    ):
                        continue
                    if re.match(BANK_CONFIG["TSB"]["date_regex"], line):
                        if current:
                            transactions.append(current)
                        current = {
                            "date": line[:9].strip(),
                            "payment_type": "",
                            "details": "",
                            "money_out": "",
                            "money_in": "",
                            "balance": "",
                        }
                        logging.debug(f"New transaction started with date: {line[:9]}")
                        # Parse transaction line
                        parts = line.split("|")
                        if len(parts) >= 6:
                            current["date"] = parts[0].strip()
                            current["payment_type"] = parts[1].strip()
                            current["details"] = parts[2].strip().replace("<br>", " ")
                            current["money_out"] = (
                                parts[3].strip().replace(",", "").replace("$", "")
                            )
                            current["money_in"] = (
                                parts[4].strip().replace(",", "").replace("$", "")
                            )
                            current["balance"] = (
                                parts[5].strip().replace(",", "").replace("$", "")
                            )
                            if "OD" in current["balance"]:
                                current["balance"] = (
                                    "-" + current["balance"].replace("OD", "").strip()
                                )
                    elif current:
                        parts = line.split("|")
                        if len(parts) >= 6:
                            current["date"] = parts[0].strip()
                            current["payment_type"] = parts[1].strip()
                            current["details"] = parts[2].strip().replace("<br>", " ")
                            current["money_out"] = (
                                parts[3].strip().replace(",", "").replace("$", "")
                            )
                            current["money_in"] = (
                                parts[4].strip().replace(",", "").replace("$", "")
                            )
                            current["balance"] = (
                                parts[5].strip().replace(",", "").replace("$", "")
                            )
                            if "OD" in current["balance"]:
                                current["balance"] = (
                                    "-" + current["balance"].replace("OD", "").strip()
                                )
                            logging.debug(f"Parsed transaction: {current}")
                        else:
                            current["details"] = (
                                current["details"] + " " + line
                            ).strip()
                            logging.debug(f"Appended to details: {line}")
                if current:
                    transactions.append(current)
    except Exception as e:
        logging.error(f"Failed to extract TSB transactions from {pdf_path}: {e}")
    return transactions


def normalize_tsb(
    transactions: List[Dict[str, str]], pdf_name: str
) -> List[Dict[str, any]]:
    normalized = []
    statement_month = "Apr 2024"  # Hardcoded for TSB_April_2025.pdf
    year = 2024
    for row in transactions:
        try:
            date_str = row.get("date", "")
            if not date_str:
                logging.debug(f"Skipping row with no date: {row}")
                continue
            try:
                date_obj = datetime.strptime(
                    date_str, BANK_CONFIG["TSB"]["date_format"]
                )
            except ValueError:
                logging.debug(f"Invalid date format: {date_str}. Skipping row: {row}")
                continue
            date = date_obj.replace(year=year).strftime("%Y-%m-%d")
            description = row.get("details", "").strip()
            money_in = float(row.get("money_in", "0")) if row.get("money_in") else 0.0
            money_out = (
                float(row.get("money_out", "0")) if row.get("money_out") else 0.0
            )
            balance = float(row.get("balance", "0")) if row.get("balance") else 0.0
            payment_type = row.get("payment_type", "").strip().upper()
            trans_type = (
                "Payment"
                if money_out > 0 and payment_type == "FASTER PAYMENT"
                else (
                    "Credit"
                    if money_in > 0 and payment_type == "FASTER PAYMENT"
                    else (
                        "Direct Debit"
                        if payment_type == "DIRECT DEBIT"
                        else (
                            "Deposit"
                            if payment_type == "CASH DEPOSIT"
                            else (
                                "Direct Credit"
                                if payment_type == "DIRECT CREDIT"
                                else (
                                    "Transfer In"
                                    if payment_type == "TRANSFER" and money_in > 0
                                    else (
                                        "Fee"
                                        if payment_type == "SERVICE CHARGES"
                                        else (
                                            "Withdrawal"
                                            if payment_type == "CASH WITHDRAWAL"
                                            else "Unknown"
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
            if description or balance or money_in or money_out:
                normalized.append(
                    {
                        "Date": date,
                        "Transaction Type": trans_type,
                        "Money In": money_in,
                        "Money Out": money_out,
                        "Bank Name": "TSB",
                        "Statement Month": statement_month,
                        "Description": description,
                        "Balance": balance,
                    }
                )
                logging.debug(
                    f"Normalized transaction: {date}, {trans_type}, {money_in}, {money_out}, {description}, {balance}"
                )
            else:
                logging.debug(f"Skipped empty transaction: {row}")
        except Exception as e:
            logging.debug(f"Failed to normalize TSB row: {row} Error: {e}")
    return normalized


def setup_directories(
    input_dir: str = "bank_statements", output_dir: str = "output"
) -> None:
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Input directory: {input_dir}, Output directory: {output_dir}")


def process_statements(
    input_dir: str = "bank_statements", output_dir: str = "output"
) -> None:
    setup_directories(input_dir, output_dir)
    all_transactions = []
    pdf_file = "TSB_April_2025.pdf"
    pdf_path = os.path.join(input_dir, pdf_file)
    if not os.path.exists(pdf_path):
        logging.error(f"File not found: {pdf_path}")
        return
    logging.info(f"Processing {pdf_file}")

    raw_data = extract_tsb_transactions(pdf_path, use_provided_ocr=True)
    normalized = normalize_tsb(raw_data, pdf_file)
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
            f"tsb_april_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
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
