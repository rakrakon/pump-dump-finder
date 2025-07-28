import os
import time
from datetime import date, datetime
import logging
import pandas as pd
from edgar import Company

import sec_utils

MAX_DAYS_SINCE_IPO = 365 / 2
FETCH_TIMEOUT = 10
MAX_RETRIES = 4
RETRY_DELAY_SEC = 30

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

current_date = date.today()

def fetch_companies_data(cik_symbol_dict):
    companies = []
    logging.info("Starting to fetch Company Data..")

    for symbol, cik in cik_symbol_dict.items():
        if cik is None:
            continue

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                company = Company(symbol, cik, FETCH_TIMEOUT)
                companies.append(company)
                logging.info(f"Fetched data for company: {symbol} (CIK: {cik})")
                break
            except Exception as e:
                logging.warning(f"[Attempt {attempt}] Failed to fetch {symbol} (CIK: {cik}): {e}")
                if attempt < MAX_RETRIES:
                    logging.info(f"Retrying {symbol} after {RETRY_DELAY_SEC} seconds...")
                    time.sleep(RETRY_DELAY_SEC)
                else:
                    logging.error(f"Giving up on {symbol} after {MAX_RETRIES} attempts.")
    logging.info("DONE!")
    return companies

def get_filtered_companies(foreign_companies: pd.DataFrame):
    symbols = foreign_companies["symbol"].tolist()
    cik_symbol_dict = sec_utils.get_ciks_by_symbols(symbols)

    companies = fetch_companies_data(cik_symbol_dict)

    logging.info("Filtering Companies..")
    filtered_companies = []
    for idx, company in enumerate(companies, 1):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                latest_filing_date = sec_utils.get_latest_prospectus_filing(company)

                if latest_filing_date is None:
                    continue

                is_recent = is_company_ipo_recent(latest_filing_date)
                if is_recent:
                    logging.info(f"Recent Filing found for {company.name}, Appending..")

                    foreign_companies.loc[foreign_companies["symbol"] == company.name, "cik"] = company.cik
                    foreign_companies.loc[foreign_companies["symbol"] == company.name, "recent_ipo_date"] = latest_filing_date.strftime("%Y-%m-%d")

                    filtered_companies.append(
                        foreign_companies.loc[foreign_companies["symbol"] == company.name].iloc[0].to_dict()
                    )
                break
            except Exception as e:
                logging.warning(f"[Attempt {attempt}] Failed to fetch {company.name} : {e}")
                if attempt < MAX_RETRIES:
                    logging.info(f"Retrying {company.name} after {RETRY_DELAY_SEC} seconds...")
                    time.sleep(RETRY_DELAY_SEC)
                else:
                    logging.error(f"Giving up on {company.name} after {MAX_RETRIES} attempts.")
    logging.info("DONE!")
    return filtered_companies

def is_company_ipo_recent(ipo_date: datetime.date):
    days_since_ipo = current_date - ipo_date
    return days_since_ipo.days < MAX_DAYS_SINCE_IPO


def ipo_filter():
    logging.info("Reading foreign_companies json..")
    foreign_companies = pd.read_json("data/foreign_companies_on_us_exchanges.json")

    logging.info("Filtering Companies..")
    recently_ipoed_companies = get_filtered_companies(foreign_companies)

    recently_ipoed_df = pd.DataFrame(recently_ipoed_companies)

    os.makedirs("data", exist_ok=True)
    recently_ipoed_df.to_json("data/recently_ipoed.json", orient="records", lines=False, indent=4)

if __name__ == "__main__":
    ipo_filter()
