from datetime import datetime

import requests

from edgar import Company

FILE_TYPES = ["424B5", "424B4"]


def get_ciks_by_symbols(symbols):
    """
    Given a list of ticker symbols, return a dict: { symbol: cik or None }
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {"User-Agent": "YourApp (Your Name; email@example.com)"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    symbol_to_cik = {}
    symbols_lower = set(s.lower() for s in symbols)

    for entry in data.values():
        ticker_lower = entry["ticker"].lower()
        if ticker_lower in symbols_lower:
            symbol_to_cik[entry["ticker"].upper()] = str(entry["cik_str"]).zfill(10)

    for s in symbols:
        if s.upper() not in symbol_to_cik:
            symbol_to_cik[s.upper()] = None

    return symbol_to_cik


def get_latest_file_date_from_html(html_content, filing_type):
    rows = html_content.xpath('//table[@class="tableFile2"]/tr[position()>1]')

    filing_dates = []
    for row in rows:
        form_type = row.xpath('./td[1]/text()')
        filing_date = row.xpath('./td[4]/text()')

        if form_type and form_type[0].strip() == filing_type and filing_date:
            filing_dates.append(filing_date[0].strip())

    if filing_dates:
        latest_date = filing_dates[0]
        return latest_date
    else:
        return None


def get_latest_prospectus_filing(company: Company):
    filings_html = [company.get_all_filings(file_type) for file_type in FILE_TYPES]

    dates = [
        get_latest_file_date_from_html(filing_html, filing_type=file_type)
        for filing_html, file_type in zip(filings_html, FILE_TYPES)
    ]

    valid_dates = [
        datetime.fromisoformat(date_str).date()
        for date_str in dates if date_str
    ]

    return max(valid_dates) if valid_dates else None

