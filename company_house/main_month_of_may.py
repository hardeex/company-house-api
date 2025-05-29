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

def extract_and_filter_data(companies, start_date, end_date, include_all_types=True):
    """Extract relevant fields and filter by RM postcodes and incorporation date."""
    filtered = []
    logging.info(f"Processing {len(companies)} companies")
    for company in companies:
        if not include_all_types and company.get("company_type") != "ltd":
            logging.debug(f"Skipping {company.get('company_name', 'N/A')}: Not a limited company ({company.get('company_type')})")
            continue

        # Check incorporation date
        creation_date_str = company.get("date_of_creation", "")
        if not creation_date_str:
            logging.debug(f"Skipping {company.get('company_name', 'N/A')}: No creation date")
            continue
        try:
            creation_date = datetime.strptime(creation_date_str, "%Y-%m-%d")
            if not (start_date <= creation_date <= end_date):
                logging.debug(f"Skipping {company.get('company_name', 'N/A')}: Creation date {creation_date_str} outside range")
                continue
        except ValueError:
            logging.debug(f"Skipping {company.get('company_name', 'N/A')}: Invalid creation date {creation_date_str}")
            continue

        address = company.get("registered_office_address", {})
        postcode = address.get("postal_code", "").upper()
        logging.debug(f"Company: {company.get('company_name', 'N/A')}, Postcode: {postcode}")
        if postcode and any(postcode.startswith(rm) for rm in RM_POSTCODES):
            logging.info(f"Found RM postcode match: {company.get('company_name', 'N/A')} ({postcode})")
        else:
            logging.debug(f"Skipping {company.get('company_name', 'N/A')}: No RM postcode ({postcode})")
            continue

        filtered.append(
            {
                "company_name": company.get("company_name", "N/A"),
                "company_number": company.get("company_number", "N/A"),
                "status": company.get("company_status", "N/A"),
                "date_of_creation": creation_date_str,
                "postcode": postcode,
                "sic_codes": ", ".join(company.get("sic_codes", [])) or "N/A",
                "company_type": company.get("company_type", "N/A"),
            }
        )
    logging.info(f"Filtered {len(filtered)} companies with RM postcodes, {'all' if include_all_types else 'ltd'} type, and created between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}")
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
    """Main function to fetch and process new RM postcode companies created this week or month."""
    today = datetime.now()
    if frequency == "weekly":
        # Start from Monday of the current week
        start_date = today - timedelta(days=today.weekday())
        end_date = today
    elif frequency == "monthly":
        # Start from the 1st of the current month
        start_date = today.replace(day=1)
        end_date = today
    else:
        logging.error("Invalid frequency. Use 'weekly' or 'monthly'.")
        exit(1)
    
    logging.info(f"Fetching companies incorporated between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}")
    all_data = []
    include_all_types = True  # Include all company types

    for location in LOCATIONS:
        logging.info(f"Starting to process location: {location}")
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

            filtered = extract_and_filter_data(companies, start_date, end_date, include_all_types)
            all_data.extend(filtered)

            cursor = result.get("next_cursor")
            logging.debug(f"Next cursor for {location}: {cursor}")
            if not cursor:
                logging.info(f"No more pages available for {location} (next_cursor is null).")
                break

            page_count += 1
            time.sleep(0.5)  # Respect rate limit (~2 req/sec)

    logging.info(f"Processed all locations: {', '.join(LOCATIONS)}")
    save_to_csv(all_data, frequency, today)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch new companies in RM postcodes.")
    parser.add_argument(
        "--frequency",
        choices=["weekly", "monthly"],
        default="monthly",
        help="Time frame to check for new companies: 'weekly' (this week) or 'monthly' (this month)."
    )
    args = parser.parse_args()
    main(args.frequency)
    