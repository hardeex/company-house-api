Overdue Companies Extractor
Extracts active companies with overdue accounts in RM postcodes (RM1–RM20, or RM9 only) from the Companies House Public Data API. Hosted on GitHub at github.com/hardeex/overdue-companies-extractor.
Overview
This project provides Python scripts to fetch data on active companies with overdue accounts in RM postcodes, filter by company type and location, and save results to CSV files. Ideal for analyzing companies in Romford, Dagenham, and nearby areas.
Features

Filter by company type (limited or all types), postcode, and account status.
Deduplication in most scripts to ensure unique records.
Outputs include company name, number, status, creation date, postcode, SIC codes, and (in one script) next due date.
Handles API rate limits and errors.
Detailed logging for debugging.
CSV output compatible with Excel, Google Sheets, etc.

Scripts and Outputs
1. main.py

Output: overdue_companies_all_limited_rm.csv
Description: Limited companies with overdue accounts in RM1–RM20.
Example: ~150 companies, e.g., SEABROOK WAREHOUSING LIMITED (RM20 3LG).
Run: python3 main.py
Notes: No deduplication; may include duplicates.

2. main_all_types.py

Output: overdue_companies_rm_all_types.csv
Description: All company types with overdue accounts in RM1–RM20.
Example: 160–200 companies, e.g., G & P DIAMONDS LP.
Run: python3 main_all_types.py
Notes: Deduplicates by company_number.

3. main_filtered.py

Output: overdue_companies_rm_filtered.csv
Description: Limited companies in RM1–RM20, excluding "active-proposal-to-strike-off", with next due date.
Example: 100–140 companies, e.g., with next_due 2023-10-31.
Run: python3 main_filtered.py
Notes: Deduplicates; includes next_due.

4. main_limited.py

Output: overdue_companies_limited_rm.csv
Description: Limited companies in RM1–RM20, deduplicated.
Example: ~150 companies.
Run: python3 main_limited.py
Notes: Similar to main.py but removes duplicates.

5. main_r9.py

Output: overdue_companies_rm9.csv
Description: Limited companies in RM9 (Dagenham).
Example: 10–20 companies, e.g., DAAC CONSTRUCTION LIMITED (RM9 3DT).
Run: python3 main_r9.py
Notes: Deduplicates; RM9 only.

CSV File Contents

Columns (all CSVs): company_name, company_number, status, date_of_creation, postcode, sic_codes, company_type.
Additional Column (main_filtered.py): next_due.
Locations: Romford (RM1–RM7), Dagenham (RM8–RM10), Hornchurch (RM11–RM12), Upminster (RM13–RM14), South Ockendon (RM15), Grays (RM16–RM17), Tilbury (RM18), Purfleet (RM19), West Thurrock (RM20). main_r9.py covers only Dagenham (RM9).

Setup Instructions

Clone Repository:git clone https://github.com/hardeex/overdue-companies-extractor.git
cd overdue-companies-extractor


Install Dependencies:pip install requests pandas python-dotenv


Set Up API Key:
Get an API key from developer.company-information.service.gov.uk.
Create a .env file:COMPANIES_HOUSE_API_KEY=your_api_key_here




Verify: Ensure Python 3.6+, internet access, and .env file are ready.

Running the Scripts

Navigate to the project directory:cd overdue-companies-extractor


Run a script:python3 main.py


Check the CSV file in the project directory.
Review terminal logs for progress or errors.

Example Output
For overdue_companies_rm_filtered.csv:
company_name,company_number,status,date_of_creation,postcode,sic_codes,company_type,next_due
SEABROOK WAREHOUSING LIMITED,12345678,active,2010-01-01,RM20 3LG,96090,ltd,2023-10-31
DAAC CONSTRUCTION LIMITED,87654321,active,2015-06-15,RM9 3DT,43390,ltd,2024-12-31

For overdue_companies_rm9.csv:
company_name,company_number,status,date_of_creation,postcode,sic_codes,company_type
DAAC CONSTRUCTION LIMITED,87654321,active,2015-06-15,RM9 3DT,43390,ltd

Summary of Differences



Script Name
Company Types
Postcodes
Locations
Output File
Deduplication
Additional Filters
Extra Output Fields



main.py
Ltd (default)
RM1–RM20
All listed locations
overdue_companies_all_limited_rm.csv
No
None
None


main_all_types.py
All types
RM1–RM20
All listed locations
overdue_companies_rm_all_types.csv
Yes
None
None


main_filtered.py
Ltd (default)
RM1–RM20
All listed locations
overdue_companies_rm_filtered.csv
Yes
Excludes "active-proposal-to-strike-off"
next_due


main_limited.py
Ltd (default)
RM1–RM20
All listed locations
overdue_companies_limited_rm.csv
Yes
None
None


main_r9.py
Ltd (default)
RM9 only
Dagenham only
overdue_companies_rm9.csv
Yes
None
None


Notes

Data Completeness: "N/A" may appear for missing data.
Duplicates: Only overdue_companies_all_limited_rm.csv may have duplicates.
Overdue Accounts: All companies have overdue accounts.
Customization: Edit include_all_types, RM_POSTCODES, or LOCATIONS in scripts for custom filters.
Rate Limits: Scripts handle API limits with retries and delays.

Contributing
Fork, branch, commit, and submit a pull request. Follow PEP 8 and add clear comments. Report issues on GitHub.
License
MIT License. See LICENSE.
Contact
Use GitHub Issues or email webmasterjdd@gmail.com. Search companies by company_number on Companies House.
