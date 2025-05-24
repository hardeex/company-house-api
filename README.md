Overdue Companies Extractor

Extracts active companies with overdue accounts and RM postcodes (RM1–RM20) from the Companies House Public Data API.


Scripts

Run each script to generate a specific CSV:

    main.py:
        Output: overdue_companies_rm.csv
        Description: Limited companies (ltd) with overdue accounts, RM1–RM20.
        Example: 151 companies (e.g., SEABROOK WAREHOUSING LIMITED, RM20 3LG).
        Run: python3 main.py
    main_all_types.py:
        Output: overdue_companies_rm_all_types.csv
        Description: All company types (e.g., ltd, limited-partnership, private-unlimited) with overdue accounts, RM1–RM20.
        Example: 160–200 companies, including G & P DIAMONDS LP (limited-partnership).
        Run: python3 main_all_types.py
    main_rm9.py:
        Output: overdue_companies_rm9.csv
        Description: Limited companies (ltd) with overdue accounts in RM9 (Dagenham).
        Example: 10–20 companies (e.g., DAAC CONSTRUCTION LIMITED, RM9 3DT).
        Run: python3 main_rm9.py
    main_filtered.py:
        Output: overdue_companies_rm_filtered.csv
        Description: Limited companies (ltd) with overdue accounts, RM1–RM20, excluding active - active proposal to strike off, includes next_due date.
        Example: 100–140 companies with next_due (e.g., 2023-10-31).
        Run: python3 main_filtered.py

Output

    Columns (all CSVs): company_name, company_number, status, date_of_creation, postcode, sic_codes, company_type
    Additional Column (main_filtered.py): next_due
    Locations: Romford (RM1–RM7), Dagenham (RM8–RM10), Hornchurch (RM11–RM12), Upminster (RM13–RM14), South Ockendon (RM15), Grays (RM16–RM17), Tilbury (RM18), Purfleet (RM19), West Thurrock (RM20).




    Summary Table of Differences

Script Name	Company Types	Postcodes	Locations	Output File	Deduplication	Additional Filters	Extra Output Fields
main.py (1/6)	Ltd (default)	RM1–RM20	All listed locations	overdue_companies_rm.csv	No	None	None
main_all_type	All types	RM1–RM20	All listed locations	overdue_companies_rm_all_types.csv	Yes	None	None
main_filtered	Ltd (default)	RM1–RM20	All listed locations	overdue_companies_rm_filtered.csv	Yes	Excludes "active-proposal-to-strike-off"	next_due
main_limited	Ltd (default)	RM1–RM20	All listed locations	overdue_companies_limited_rm.csv	Yes	None	None
main_r9	Ltd (default)	RM9 only	Dagenham only	overdue_companies_rm9.csv	Yes