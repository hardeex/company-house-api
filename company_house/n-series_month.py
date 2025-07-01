import requests
import pandas as pd
import time
import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
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
FILING_ENDPOINT = "/company/{}/filing-history"
MAX_RETRIES = 5

# Define North London postcodes and their primary locations
POSTCODE_LOCATIONS = {
    "N1": ["Islington", "London"],
    "N2": ["East Finchley", "London"],
    "N3": ["Finchley", "London"],
    "N4": ["Finsbury Park", "London"],
    "N5": ["Highbury", "London"],
    "N6": ["Highgate", "London"],
    "N7": ["Holloway", "London"],
    "N8": ["Hornsey", "London"],
    "N9": ["Edmonton", "London"],
    "N10": ["Muswell Hill", "London"],
    "N11": ["New Southgate", "London"],
    "N12": ["North Finchley", "Barnet", "London"],
    "N13": ["Palmers Green", "London"],
    "N14": ["Southgate", "London"],
    "N15": ["Seven Sisters", "London"],
    "N16": ["Stoke Newington", "London"],
    "N17": ["Tottenham", "London"],
    "N18": ["Upper Edmonton", "London"],
    "N19": ["Archway", "London"],
    "N20": ["Whetstone", "London"],
    "N21": ["Winchmore Hill", "London"],
    "N22": ["Wood Green", "London"],
}


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
        logging.info(
            f"Received {len(data.get('items', []))} companies, total hits: {total_results}"
        )
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
    """Check if a company has overdue accounts and return next_due date."""
    company_url = f"{BASE_URL}/company/{company_number}"
    try:
        response = requests.get(company_url, auth=HTTPBasicAuth(API_KEY, ""))
        response.raise_for_status()
        company_data = response.json()
        if company_data.get("company_status") != "active":
            logging.debug(
                f"Skipping {company_number}: Not active ({company_data.get('company_status')})"
            )
            return False, None
        accounts = company_data.get("accounts", {})
        next_due = accounts.get("next_due")
        if next_due:
            due_date = datetime.strptime(next_due, "%Y-%m-%d")
            if due_date < datetime.now():
                logging.info(
                    f"Overdue accounts detected for {company_number}: next_due={next_due}"
                )
                return True, next_due
            else:
                logging.debug(
                    f"No overdue accounts for {company_number}: next_due={next_due}"
                )
        else:
            logging.debug(f"No next_due date for {company_number}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch company details for {company_number}: {e}")
        return False, None

    # Fallback: Check filing history
    url = f"{BASE_URL}{FILING_ENDPOINT.format(company_number)}"
    try:
        response = requests.get(
            url, auth=HTTPBasicAuth(API_KEY, ""), params={"category": "accounts"}
        )
        response.raise_for_status()
        filings = response.json().get("items", [])
        for filing in filings:
            if (
                filing.get("category") == "accounts"
                and filing.get("description", "").lower().find("late") > -1
            ):
                logging.info(
                    f"Overdue accounts detected in filing history for {company_number}"
                )
                return True, next_due
        logging.debug(f"No late filings in history for {company_number}")
        return False, None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch filing history for {company_number}: {e}")
        return False, None


def extract_and_filter_data(companies, postcode_prefix, include_all_types=True):
    """Extract relevant fields and filter by specified N postcode and overdue accounts."""
    filtered = []
    logging.info(
        f"Processing {len(companies)} companies for postcode {postcode_prefix}"
    )
    for company in companies:
        address = company.get("registered_office_address", {})
        postcode = address.get("postal_code", "").upper().replace(" ", "")
        logging.debug(
            f"Company: {company.get('company_name', 'N/A')}, Postcode: {postcode}"
        )

        # Check if postcode matches the specified N postcode (e.g., N1, N12)
        if not postcode or not postcode.startswith(postcode_prefix.replace(" ", "")):
            continue

        company_number = company.get("company_number", "")
        has_overdue, next_due = check_overdue_accounts(company_number)
        if not has_overdue:
            continue

        # Extract full address
        address_lines = [
            address.get("address_line_1", ""),
            address.get("address_line_2", ""),
            address.get("locality", ""),
            address.get("region", ""),
            address.get("country", ""),
        ]
        full_address = ", ".join([line for line in address_lines if line])

        filtered.append(
            {
                "company_name": company.get("company_name", "N/A"),
                "company_number": company_number,
                "status": company.get("company_status", "N/A"),
                "date_of_creation": company.get("date_of_creation", "N/A"),
                "postcode": postcode,
                "full_address": full_address or "N/A",
                "sic_codes": ", ".join(company.get("sic_codes", [])) or "N/A",
                "company_type": company.get("company_type", "N/A"),
                "next_due": next_due or "N/A",
            }
        )
    logging.info(
        f"Filtered {len(filtered)} companies with {postcode_prefix} postcodes and overdue accounts"
    )
    return filtered


def save_to_csv(data, postcode_prefix, timestamp):
    """Save extracted data to CSV with timestamped filename."""
    if not data:
        logging.warning(f"No data to save for {postcode_prefix}.")
        return
    filename = f"overdue_companies_{postcode_prefix.lower()}_month_{timestamp.strftime('%Y-%m-%d')}.csv"
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset="company_number")  # Deduplicate
    postcodes = (
        df["postcode"].str.extract(rf"^({postcode_prefix}\s?\d?)").dropna()[0].unique()
    )
    logging.info(f"Found {postcode_prefix} postcodes: {', '.join(postcodes)}")
    df.to_csv(filename, index=False)
    logging.info(f"Saved {len(df)} companies to {filename}")


def main():
    """Main function to fetch and process companies with overdue accounts for all N postcodes."""
    timestamp = datetime.now()
    start_date = timestamp - timedelta(days=30)  # Last 30 days
    end_date = timestamp

    for postcode, locations in POSTCODE_LOCATIONS.items():
        all_data = []
        logging.info(f"Processing postcode: {postcode}")
        for location in locations:
            logging.info(f"Processing location: {location} for {postcode}")
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

                filtered = extract_and_filter_data(
                    companies, postcode, include_all_types=True
                )
                all_data.extend(filtered)

                cursor = result.get("next_cursor")
                if not cursor:
                    logging.info(
                        f"No more pages available for {location} (next_cursor is null)."
                    )
                    break

                page_count += 1
                time.sleep(0.5)  # Respect rate limit (~2 req/sec)

        save_to_csv(all_data, postcode, timestamp)


if __name__ == "__main__":
    main()
