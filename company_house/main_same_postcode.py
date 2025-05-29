import requests
import pandas as pd
import time
import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import argparse
from requests.auth import HTTPBasicAuth

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables
load_dotenv()
API_KEY = os.getenv('COMPANIES_HOUSE_API_KEY')
if not API_KEY:
    logging.error("API_KEY not found in .env file. Please set COMPANIES_HOUSE_API_KEY.")
    exit(1)
logging.info(f"API Key loaded: {API_KEY[:4]}...{API_KEY[-4:]}")

BASE_URL = "https://api.company-information.service.gov.uk"
SEARCH_ENDPOINT = "/advanced-search/companies"
FILING_ENDPOINT = "/company/{}/filing-history"
MAX_RETRIES = 5
RM_POSTCODES = [f"RM{i}" for i in range(1, 21)]  # RM1–RM20
LOCATIONS = [
    "Romford", "Dagenham", "Hornchurch", "Upminster", "South Ockendon",
    "Grays", "Tilbury", "Purfleet", "West Thurrock"
]

def fetch_companies(location, start_date, end_date, cursor=None, retries=0):
    """Fetch companies for a given location and date range from Companies House API."""
    url = f"{BASE_URL}{SEARCH_ENDPOINT}"
    params = {
        "size": 100,
        "cursor": cursor,
        "company_status": "active",
        "location": location,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
    }

    try:
        logging.info(f"Making request to {url} with params {params}")
        response = requests.get(url, auth=HTTPBasicAuth(API_KEY, ""), params=params)
        response.raise_for_status()
        data = response.json()
        total_results = data.get("total_results", data.get("hits", "N/A"))
        if total_results == "N/A":
            logging.warning(f"total_results missing in response: {data}")
        logging.info(f"Received {len(data.get('items', []))} companies, total hits: {total_results}")
        return data
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            if retries >= MAX_RETRIES:
                logging.error("Max retries reached for rate limit.")
                return None
            wait_time = int(response.headers.get("Retry-After", 60))
            logging.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            return fetch_companies(location, start_date, end_date, cursor, retries + 1)
        logging.error(f"HTTP error: {e} - {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

def check_overdue_accounts(company_number):
    """Check if a company has overdue accounts."""
    company_url = f"{BASE_URL}/company/{company_number}"
    try:
        response = requests.get(company_url, auth=HTTPBasicAuth(API_KEY, ""))
        response.raise_for_status()
        company_data = response.json()
        if company_data.get("company_status") != "active":
            logging.debug(f"Skipping {company_number}: Not active ({company_data.get('company_status')})")
            return False
        accounts = company_data.get("accounts", {})
        next_due = accounts.get("next_due")
        if next_due:
            due_date = datetime.strptime(next_due, "%Y-%m-%d")
            if due_date < datetime.now():
                logging.info(f"Overdue accounts detected for {company_number}: next_due={next_due}")
                return True
            else:
                logging.debug(f"No overdue accounts for {company_number}: next_due={next_due}")
        else:
            logging.debug(f"No next_due date for {company_number}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch company details for {company_number}: {e}")
        return False

    # Fallback: Check filing history
    url = f"{BASE_URL}{FILING_ENDPOINT.format(company_number)}"
    try:
        response = requests.get(url, auth=HTTPBasicAuth(API_KEY, ""), params={"category": "accounts"})
        response.raise_for_status()
        filings = response.json().get("items", [])
        for filing in filings:
            if filing.get("category") == "accounts" and filing.get("description", "").lower().find("late") > -1:
                logging.info(f"Overdue accounts detected in filing history for {company_number}")
                return True
        logging.debug(f"No late filings in history for {company_number}")
        return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch filing history for {company_number}: {e}")
        return False

def extract_and_filter_data(companies, include_all_types=True):
    """Extract relevant fields and filter by RM postcodes, overdue accounts, and company type."""
    filtered = []
    logging.info(f"Processing {len(companies)} companies")
    for company in companies:
        if not include_all_types and company.get("company_type") != "ltd":
            logging.debug(f"Skipping {company.get('company_name', 'N/A')}: Not a limited company ({company.get('company_type')})")
            continue

        address = company.get("registered_office_address", {})
        postcode = address.get("postal_code", "").upper()
        logging.debug(f"Company: {company.get('company_name', 'N/A')}, Postcode: {postcode}")
        if postcode and any(postcode.startswith(rm) for rm in RM_POSTCODES):
            logging.info(f"Found RM postcode match: {company.get('company_name', 'N/A')} ({postcode})")

        if not postcode or not any(postcode.startswith(rm) for rm in RM_POSTCODES):
            continue

        company_number = company.get("company_number", "")
        if not check_overdue_accounts(company_number):
            continue

        filtered.append(
            {
                "company_name": company.get("company_name", "N/A"),
                "company_number": company_number,
                "status": company.get("company_status", "N/A"),
                "date_of_creation": company.get("date_of_creation", "N/A"),
                "postcode": postcode,
                "sic_codes": ", ".join(company.get("sic_codes", [])) or "N/A",
                "company_type": company.get("company_type", "N/A"),
            }
        )
    logging.info(f"Filtered {len(filtered)} companies with RM postcodes, overdue accounts, and {'all' if include_all_types else 'ltd'} type")
    return filtered

def save_to_csv(data, frequency, timestamp):
    """Save extracted data to CSV with timestamped filename."""
    if not data:
        logging.warning("No data to save.")
        return
    filename = f"new_companies_rm_{frequency}_{timestamp.strftime('%Y-%m-%d')}.csv"
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset="company_number")  # Deduplicate
    postcodes = df["postcode"].str.extract(r"^(RM\d+)").dropna()[0].unique()
    logging.info(f"Found RM postcodes: {', '.join(postcodes)}")
    df.to_csv(filename, index=False)
    logging.info(f"Saved {len(df)} companies to {filename}")

def main(frequency):
    """Main function to fetch and process new RM postcode companies."""
    today = datetime.now()
    if frequency == "daily":
        start_date = today - timedelta(days=1)
    elif frequency == "weekly":
        start_date = today - timedelta(days=7)
    else:
        logging.error("Invalid frequency. Use 'daily' or 'weekly'.")
        exit(1)
    end_date = today
    all_data = []
    include_all_types = True  # Include all company types

    for location in LOCATIONS:
        logging.info(f"Processing location: {location}")
        cursor = "*"
        page_count = 0
        while cursor:
            logging.info(f"Fetching page {page_count + 1} for {location}...")
            result = fetch_companies(location, start_date, end_date, cursor)
            if not result:
                logging.error(f"Failed to fetch data for {location}. Skipping.")
                break

            companies = result.get("items", [])
            if not companies:
                logging.info(f"No more companies to fetch for {location}.")
                break

            filtered = extract_and_filter_data(companies, include_all_types)
            all_data.extend(filtered)

            cursor = result.get("next_cursor")
            if not cursor:
                logging.info(f"No more pages available for {location} (next_cursor is null).")
                break

            page_count += 1
            time.sleep(0.5)  # Respect rate limit (~2 req/sec)

    save_to_csv(all_data, frequency, today)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch new companies in RM postcodes.")
    parser.add_argument(
        "--frequency",
        choices=["daily", "weekly"],
        default="daily",
        help="Frequency to check for new companies: 'daily' or 'weekly'."
    )
    args = parser.parse_args()
    main(args.frequency)