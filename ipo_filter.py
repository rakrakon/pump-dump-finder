import os
from datetime import date
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,  # Enable INFO level and above
    format='%(asctime)s - %(levelname)s - %(message)s'
)

MAX_DAYS_SINCE_IPO = 365 / 2

current_date = date.today()


def get_filtered_companies(foreign_companies: pd.DataFrame):
    return list(
        filter(is_company_ipo_recent, foreign_companies.to_dict(orient='records'))
    )


def is_company_ipo_recent(company_dict: dict):
    symbol = company_dict['symbol']
    logging.info(f"Checking ipo date of {symbol}..")
    if company_dict["ipo"] == "":
        logging.info(f"No ipo date found for {symbol}, Skipping..")
        return False

    year, month, day = map(int, company_dict["ipo"].split("-"))
    ipo_date = date(year, month, day)
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
