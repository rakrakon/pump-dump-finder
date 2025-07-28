import time
import pandas as pd
import os
import logging
import finnhub
from dotenv import load_dotenv
from data_fetching.us_tickers import OUTPUT_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("stock_filter.log"),
        logging.StreamHandler()
    ]
)

IRRELEVANT_COUNTRIES = ["US", "CA", "GB", "IE", "NL", "AU", "FI", "IL", "DE", "GR", "IT", "DK", "CH"]
MAX_OUTSTANDING_SHARES = 300.0  # (Million)
MAX_STOCK_PRICE = 40.0           # Max price to allow
MAX_MARKET_CAP = 300.0     # (Miilion)

load_dotenv()
API_KEY = os.getenv("API_KEY")
finnhub_client = finnhub.Client(api_key=API_KEY)

def load_data(path: str) -> pd.DataFrame:
    logging.info(f"Loading data from {path}")
    df = pd.read_json(path)
    logging.info(f"Loaded {len(df)} records")
    return df

def filter_by_country_and_shares(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Filtering out irrelevant countries: {IRRELEVANT_COUNTRIES}")
    country_filtered = df[~df['country'].isin(IRRELEVANT_COUNTRIES)]
    logging.info(f"Records after country filter: {len(country_filtered)}")

    shares_filtered = country_filtered[country_filtered['shareOutstanding'] <= MAX_OUTSTANDING_SHARES]
    logging.info(f"Records after share outstanding filter (<= {MAX_OUTSTANDING_SHARES}M): {len(shares_filtered)}")
    return shares_filtered

def get_stock_info(ticker: str, retries: int = 4, delay: int = 60) -> dict:
    attempt = 0
    while attempt <= retries:
        try:
            logging.info(f"Fetching price & market cap for {ticker} (attempt {attempt + 1})")

            quote = finnhub_client.quote(ticker)
            price = quote.get("c")  # current price

            metrics = finnhub_client.company_basic_financials(ticker, 'all')
            market_cap = None
            if "metric" in metrics and "marketCapitalization" in metrics["metric"]:
                market_cap = metrics["metric"]["marketCapitalization"]

            logging.info(f"{ticker}: price={price}, marketCap={market_cap}")
            return {"price": price, "market_cap": market_cap}

        except Exception as e:
            logging.error(f"Error fetching info for {ticker} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                logging.info(f"Waiting {delay} seconds before retrying...")
                time.sleep(delay)
            attempt += 1

    logging.error(f"Failed to fetch info for {ticker} after {retries + 1} attempts")
    return {"price": None, "market_cap": None}

def filter_by_price_and_market_cap(df: pd.DataFrame, max_price: float, max_market_cap: int) -> pd.DataFrame:
    prices = []
    market_caps = []

    logging.info("Fetching price and market cap for each ticker...")

    for ticker in df['symbol']:
        info = get_stock_info(ticker)
        prices.append(info["price"])
        market_caps.append(info["market_cap"])

    df = df.assign(currentPrice=prices, marketCap=market_caps)
    df = df[df['currentPrice'].notnull() & df['marketCap'].notnull()]
    filtered_df = df[(df['currentPrice'] <= max_price) & (df['marketCap'] <= max_market_cap)]

    logging.info(f"Records after price (<= {max_price}) and market cap (<= {max_market_cap}) filter: {len(filtered_df)}")
    return filtered_df

def main():
    data = load_data(f"{OUTPUT_DIR}/recently_ipoed.json")
    filtered = filter_by_country_and_shares(data)
    filtered = filter_by_price_and_market_cap(filtered, MAX_STOCK_PRICE, MAX_MARKET_CAP)
    filtered.to_json("data/filtered_stocks.json", orient="records", lines=False, indent=4)
    logging.info("Filtered data saved to data/filtered_stocks.json")
    print(filtered)

if __name__ == "__main__":
    main()
