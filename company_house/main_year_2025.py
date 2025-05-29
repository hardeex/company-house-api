import requests
import pandas as pd
import time
import logging
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import argparse
from requests.auth import HTTPBasicAuth

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables
load_dotenv()
API_KEY = os.getenv("COMPANIES_HOUSE_API_KEY")
if not API_KEY:
    logging.error("API_KEY not found in .env file. Please set COMPANIES_HOUSE_API_KEY.")
    exit(1)
logging.info(f"API Key loaded: {API_KEY[:4]}...{API_KEY[-4:]}")

BASE_URL = "https://api.company-information.service.gov.uk"
SEARCH_ENDPOINT = "/advanced-search/companies"
MAX_RETRIES = 5
RETRY_DELAY = 60
RM_POSTCODES = [f"RM{i}" for i in range(1, 21)]  # RM1–RM20
LOCATIONS = [
    "Romford",
    "Dagenham",
    "Hornchurch",
    "Upminster",
    "South Ockendon",
    "Grays",
    "Tilbury",
    "Purfleet",
    "West Thurrock",
]
MAX_PAGES = 10000  # Safety limit to avoid infinite loops


def fetch_companies(
    start_date, end_date, cursor=None, start_index=0, location=None, retries=0
):
    """Fetch companies for a given date range from Companies House API."""
    url = f"{BASE_URL}{SEARCH_ENDPOINT}"
    params = {
        "size": 100,
        "cursor": cursor if cursor else "*",
        "company_status": "active",
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
    }
    if location:
        params["location"] = location
    if not cursor and start_index > 0:
        params["start_index"] = start_index

    try:
        logging.info(f"Making request to {url} with params {params}")
        response = requests.get(
            url, auth=HTTPBasicAuth(API_KEY, ""), params=params, timeout=30
        )
        response.raise_for_status()
        data = response.json()
        total_results = data.get("total_results", data.get("hits", 0))
        items = data.get("items", [])
        logging.info(f"Received {len(items)} companies, total hits: {total_results}")
        return data, total_results
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            if retries >= MAX_RETRIES:
                logging.error("Max retries reached for rate limit.")
                return None, 0
            wait_time = int(response.headers.get("Retry-After", RETRY_DELAY))
            logging.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            return fetch_companies(
                start_date, end_date, cursor, start_index, location, retries + 1
            )
        logging.error(f"HTTP error: {e} - {response.text}")
        return None, 0
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        if retries < MAX_RETRIES:
            logging.warning(
                f"Retrying after {RETRY_DELAY} seconds... (Attempt {retries + 1}/{MAX_RETRIES})"
            )
            time.sleep(RETRY_DELAY)
            return fetch_companies(
                start_date, end_date, cursor, start_index, location, retries + 1
            )
        return None, 0


def extract_and_filter_data(companies, start_date, end_date, include_all_types=True):
    """Extract relevant fields and filter by RM postcodes and incorporation date."""
    filtered = []
    logging.info(f"Processing {len(companies)} companies")
    for company in companies:
        creation_date_str = company.get("date_of_creation", "")
        address = company.get("registered_office_address", {})
        postcode = address.get("postal_code", "").upper().strip()
        logging.debug(
            f"Company: {company.get('company_name', 'N/A')}, Postcode: {postcode}, Creation date: {creation_date_str}"
        )

        if not include_all_types and company.get("company_type") != "ltd":
            logging.debug(
                f"Skipping {company.get('company_name', 'N/A')}: Not a limited company ({company.get('company_type')})"
            )
            continue

        if not creation_date_str:
            logging.debug(
                f"Skipping {company.get('company_name', 'N/A')}: No creation date"
            )
            continue
        try:
            creation_date = datetime.strptime(creation_date_str, "%Y-%m-%d")
            if not (start_date <= creation_date <= end_date):
                logging.debug(
                    f"Skipping {company.get('company_name', 'N/A')}: Creation date {creation_date_str} outside range"
                )
                continue
        except ValueError:
            logging.debug(
                f"Skipping {company.get('company_name', 'N/A')}: Invalid creation date {creation_date_str}"
            )
            continue

        if not postcode:
            logging.debug(
                f"Skipping {company.get('company_name', 'N/A')}: No postcode provided"
            )
            continue
        if any(postcode.startswith(rm) for rm in RM_POSTCODES):
            logging.info(
                f"Found RM postcode match: {company.get('company_name', 'N/A')} ({postcode})"
            )
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
        else:
            logging.debug(
                f"Skipping {company.get('company_name', 'N/A')}: No RM postcode ({postcode})"
            )

    logging.info(
        f"Filtered {len(filtered)} companies with RM postcodes, {'all' if include_all_types else 'ltd'} type, and created between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}"
    )
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


def save_cache(data, frequency, timestamp):
    """Save fetched data to a JSON cache file."""
    cache_file = f"cache_{frequency}_{timestamp.strftime('%Y-%m-%d')}.json"
    with open(cache_file, "w") as f:
        json.dump(data, f)
    logging.info(f"Saved {len(data)} companies to cache: {cache_file}")


def load_cache(frequency, timestamp):
    """Load data from a JSON cache file if it exists."""
    cache_file = f"cache_{frequency}_{timestamp.strftime('%Y-%m-%d')}.json"
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            data = json.load(f)
        logging.info(f"Loaded {len(data)} companies from cache: {cache_file}")
        return data
    return []


def main(frequency):
    """Main function to fetch and process new RM postcode companies created this year."""
    today = datetime.now()
    if frequency == "yearly":
        start_date = today.replace(month=1, day=1)
        end_date = today
    else:
        logging.error("Invalid frequency. Use 'yearly'.")
        exit(1)

    # Uncomment to test with 2024 data
    # start_date = today.replace(year=2024, month=1, day=1)
    # end_date = today.replace(year=2024, month=12, day=31)

    logging.info(
        f"Fetching companies incorporated between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}"
    )
    all_data = load_cache(frequency, today)
    include_all_types = True
    total_fetched = len(all_data)

    # Try location-based search first
    for location in LOCATIONS + [None]:  # None for UK-wide search
        location_str = location if location else "UK-wide"
        logging.info(f"Starting search for {location_str}")
        cursor = "*"
        start_index = 0
        page_count = 0

        while page_count < MAX_PAGES:
            logging.info(f"Fetching page {page_count + 1} for {location_str}...")
            result, total_results = fetch_companies(
                start_date, end_date, cursor, start_index, location
            )
            if not result:
                logging.error(f"Failed to fetch data for {location_str}. Skipping.")
                break

            companies = result.get("items", [])
            if not companies:
                logging.info(f"No more companies to fetch for {location_str}.")
                break

            filtered = extract_and_filter_data(
                companies, start_date, end_date, include_all_types
            )
            all_data.extend(filtered)
            total_fetched += len(companies)
            logging.info(
                f"Total companies fetched so far: {total_fetched}/{total_results}"
            )

            cursor = result.get("next_cursor", None)
            if cursor:
                logging.debug(f"Next cursor for {location_str}: {cursor}")
            else:
                logging.debug(
                    f"No cursor for {location_str}. Trying start_index: {start_index + 100}"
                )
                cursor = None
                start_index += 100
                if start_index >= total_results:
                    logging.info(
                        f"Reached end of results for {location_str} (start_index: {start_index}, total: {total_results})."
                    )
                    break

            page_count += 1
            save_cache(all_data, frequency, today)  # Cache after each page
            time.sleep(0.5)  # Respect rate limit (~2 req/sec)

            if total_fetched >= total_results:
                logging.info(
                    f"Fetched all {total_results} companies for {location_str}."
                )
                break

        if all_data and location:  # If location-based search finds data, skip UK-wide
            logging.info(f"Found data for {location}. Skipping UK-wide search.")
            break

    logging.info("Processed all data")
    save_to_csv(all_data, frequency, today)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch new companies in RM postcodes for 2025."
    )
    parser.add_argument(
        "--frequency",
        choices=["yearly"],
        default="yearly",
        help="Time frame to check for new companies: 'yearly' (this year, 2025).",
    )
    args = parser.parse_args()
    main(args.frequency)
