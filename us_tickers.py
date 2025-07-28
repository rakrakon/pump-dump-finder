import os

import logging
import finnhub
import time
import json
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd


load_dotenv()
API_KEY = os.getenv("API_KEY")
SAVE_EVERY = 50
RATE_LIMIT_DELAY = 1.05
OUTPUT_DIR = "data"
OUTPUT_FILE = f"{OUTPUT_DIR}/foreign_companies_on_us_exchanges.json"

os.makedirs(OUTPUT_DIR, exist_ok=True)

client = finnhub.Client(api_key=API_KEY)

logging.basicConfig(
    level=logging.INFO,  # Enable INFO level and above
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_existing_data(filepath):
    if Path(filepath).exists():
        with open(OUTPUT_FILE, "r") as f:
            foreign_companies = json.load(f)
        completed_symbols = {entry["symbol"] for entry in foreign_companies}
        return foreign_companies, completed_symbols
    else:
        return [], set()

def save_data(filepath, data):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def get_all_us_tickers():
    nasdaq_url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
    other_url  = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

    nasdaq_df = pd.read_csv(nasdaq_url, sep="|", dtype=str)
    other_df = pd.read_csv(other_url, sep="|", dtype=str)

    nasdaq_df = nasdaq_df[~nasdaq_df['Symbol'].str.contains('File Creation Time', na=False)]
    other_df = other_df[~other_df['ACT Symbol'].str.contains('File Creation Time', na=False)]

    nasdaq = nasdaq_df['Symbol'].dropna().astype(str).tolist()
    nyse_amex = other_df['ACT Symbol'].dropna().astype(str).tolist()

    all_tickers = sorted(set(nasdaq + nyse_amex))
    return all_tickers

def fetch_foreign_company_profile(symbol):
    profile = client.company_profile2(symbol=symbol)
    country = profile.get("country")
    if country and country != "US":
        return {
            "symbol": symbol,
            "name": profile.get("name"),
            "exchange": profile.get("exchange"),
            "country": country,
            "ipo": profile.get("ipo"),
            "shareOutstanding": profile.get("shareOutstanding"),
            "weburl": profile.get("weburl")
        }
    return None

def process_symbols(symbols, completed_symbols, foreign_companies):
    for idx, symbol in enumerate(symbols, 1):
        if symbol in completed_symbols:
            logging.info(f"[{idx}] {symbol} skipped (already processed)")
            continue

        try:
            company = fetch_foreign_company_profile(symbol)
            if company:
                foreign_companies.append(company)
                logging.info(f"[{idx}] {symbol} ({company['country']}) added")
            else:
                logging.info(f"[{idx}] {symbol} skipped (US-based or no country info)")
        except Exception as e:
            logging.info(f"[{idx}] Error with {symbol}: {e}")

        if idx % SAVE_EVERY == 0:
            save_data(OUTPUT_FILE, foreign_companies)
            logging.info(f"Saved progress at {idx} symbols")

        time.sleep(RATE_LIMIT_DELAY)

def main():
    logging.info("Loading existing data...")
    foreign_companies, completed_symbols = load_existing_data(OUTPUT_FILE)

    logging.info("Fetching list of US tickers...")
    symbols = get_all_us_tickers()
    logging.info(f"Tickers to process: {len(symbols)}")

    process_symbols(symbols, completed_symbols, foreign_companies)

    # Final save
    save_data(OUTPUT_FILE, foreign_companies)
    logging.info(f"\nDone! Found {len(foreign_companies)} foreign companies listed on US exchanges.")

if __name__ == "__main__":
    main()