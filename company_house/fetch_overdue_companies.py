import requests
import pandas as pd
import time
import logging
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os
from datetime import datetime, date

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
LONDON_POSTCODES = [
    *[f"E{i}" for i in range(1, 20)],  # E1-E19
    *[f"N{i}" for i in range(1, 23)],  # N1-N22
    *[f"SE{i}" for i in range(1, 29)],  # SE1-SE28
    *[f"W{i}" for i in range(1, 15)],   # W1-W14
    *[f"IG{i}" for i in range(1, 12)],  # IG1-IG11
    *[f"RM{i}" for i in range(1, 21)]   # RM1-RM20
]
LOCATIONS = [
    "London", "Romford", "Dagenham", "Hornchurch", "Upminster",
    "South Ockendon", "Grays", "Tilbury", "Purfleet", "West Thurrock",
    "Ilford", "Barking", "Woodford Green", "Chigwell"
]

def fetch_companies(location, cursor=None, retries=0):
    """Fetch companies for a given location from Companies House API."""
    url = f"{BASE_URL}{SEARCH_ENDPOINT}"
    params = {
        "size": 100,
        "cursor": cursor,
        "company_status": "active",
        "location": location,
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
            return fetch_companies(location, cursor, retries + 1)
        logging.error(f"HTTP error: {e} - {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

def check_overdue_accounts(company_number):
    """Check if a company has overdue accounts for specific time periods."""
    company_url = f"{BASE_URL}/company/{company_number}"
    try:
        response = requests.get(company_url, auth=HTTPBasicAuth(API_KEY, ""))
        response.raise_for_status()
        company_data = response.json()
        if company_data.get("company_status") != "active":
            logging.debug(f"Skipping {company_number}: Not active ({company_data.get('company_status')})")
            return None
        accounts = company_data.get("accounts", {})
        next_due = accounts.get("next_due")
        if not next_due:
            logging.debug(f"No next_due date for {company_number}")
            return None

        due_date = datetime.strptime(next_due, "%Y-%m-%d").date()
        today = date(2025, 7, 17)  # Current date for reference
        jan_2025 = date(2025, 1, 31)
        jul_2025 = date(2025, 7, 31)
        aug_2025 = date(2025, 8, 31)

        result = {
            "jan_2025": False,
            "jul_2025": False,
            "aug_2025": False,
            "next_due": next_due
        }

        if due_date < today and due_date <= jan_2025:
            result["jan_2025"] = True
            logging.info(f"January 2025 overdue accounts detected for {company_number}: next_due={next_due}")
        if due_date <= jul_2025:
            result["jul_2025"] = True
            logging.info(f"July 2025 due accounts detected for {company_number}: next_due={next_due}")
        if due_date <= aug_2025:
            result["aug_2025"] = True
            logging.info(f"August 2025 due accounts detected for {company_number}: next_due={next_due}")

        return result
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch company details for {company_number}: {e}")
        return None

def extract_and_filter_data(companies, include_all_types=True):
    """Extract relevant fields and filter by London postcodes and overdue accounts."""
    jan_data, jul_data, aug_data = [], [], []
    logging.info(f"Processing {len(companies)} companies")

    for company in companies:
        if not include_all_types and company.get("company_type") != "ltd":
            logging.debug(f"Skipping {company.get('company_name', 'N/A')}: Not a limited company ({company.get('company_type')})")
            continue

        address = company.get("registered_office_address", {})
        postcode = address.get("postal_code", "").upper()
        logging.debug(f"Company: {company.get('company_name', 'N/A')}, Postcode: {postcode}")
        if postcode and any(postcode.startswith(pc) for pc in LONDON_POSTCODES):
            logging.info(f"Found London postcode match: {company.get('company_name', 'N/A')} ({postcode})")

        if not postcode or not any(postcode.startswith(pc) for pc in LONDON_POSTCODES):
            continue

        company_number = company.get("company_number", "")
        overdue_info = check_overdue_accounts(company_number)
        if not overdue_info:
            continue

        company_data = {
            "company_name": company.get("company_name", "N/A"),
            "company_number": company_number,
            "status": company.get("company_status", "N/A"),
            "date_of_creation": company.get("date_of_creation", "N/A"),
            "postcode": postcode,
            "sic_codes": ", ".join(company.get("sic_codes", [])) or "N/A",
            "company_type": company.get("company_type", "N/A"),
            "next_due": overdue_info["next_due"]
        }

        if overdue_info["jan_2025"]:
            jan_data.append(company_data)
        if overdue_info["jul_2025"]:
            jul_data.append(company_data)
        if overdue_info["aug_2025"]:
            aug_data.append(company_data)

    logging.info(f"Filtered: January({len(jan_data)}), July({len(jul_data)}), August({len(aug_data)}) companies")
    return jan_data, jul_data, aug_data

def save_to_csv(data, filename):
    """Save extracted data to CSV."""
    if not data:
        logging.warning(f"No data to save for {filename}.")
        return
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset="company_number")
    postcodes = df["postcode"].str.extract(r"^(E\d+|N\d+|SE\d+|W\d+|IG\d+|RM\d+)").dropna()[0].unique()
    logging.info(f"Found postcodes for {filename}: {', '.join(postcodes)}")
    df.to_csv(filename, index=False)
    logging.info(f"Saved {len(df)} companies to {filename}")

def main():
    """Main function to fetch and process London postcode companies."""
    jan_data, jul_data, aug_data = [], [], []
    include_all_types = True

    for location in LOCATIONS:
        logging.info(f"Processing location: {location}")
        cursor = "*"
        page_count = 0
        while cursor:
            logging.info(f"Fetching page {page_count + 1} for {location}...")
            result = fetch_companies(location, cursor)
            if not result:
                logging.error(f"Failed to fetch data for {location}. Skipping.")
                break

            companies = result.get("items", [])
            if not companies:
                logging.info(f"No more companies to fetch for {location}.")
                break

            j_data, ju_data, a_data = extract_and_filter_data(companies, include_all_types)
            jan_data.extend(j_data)
            jul_data.extend(ju_data)
            aug_data.extend(a_data)

            cursor = result.get("next_cursor")
            if not cursor:
                logging.info(f"No more pages available for {location} (next_cursor is null).")
                break

            page_count += 1
            time.sleep(0.5)  # Respect rate limit (~2 req/sec)

    save_to_csv(jan_data, "overdue_companies_jan_2025.csv")
    save_to_csv(jul_data, "overdue_companies_jul_2025.csv")
    save_to_csv(aug_data, "overdue_companies_aug_2025.csv")

if __name__ == "__main__":
    main()