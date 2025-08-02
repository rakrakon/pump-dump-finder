from edgar import Company
from lxml import html
from lxml.html import tostring, fromstring
from urllib.parse import urljoin, urlparse
import os
import re
import requests
import logging

from sec_utils import get_ciks_by_symbols, EDGAR_USER_AGENT

BASE_OUTPUT_DIR = "data/edgar_documents"
BASE_URL = "https://www.sec.gov"

# Matches .htm files under /Archives/edgar/data/
EDGAR_DOC_PATTERN = re.compile(r"^/Archives/edgar/data/\d+/.*\.htm$")

def download_htm_documents_for_symbol(symbol: str, no_of_entries: int = 10):
    """Downloads all .htm files from recent EDGAR filings for a given symbol."""
    output_dir = os.path.join(BASE_OUTPUT_DIR, symbol.upper())
    os.makedirs(output_dir, exist_ok=True)

    try:
        symbol_to_cik = get_ciks_by_symbols([symbol])
        cik = symbol_to_cik[symbol]
        company = Company(symbol, cik)
        filings_html = company.get_all_filings(no_of_entries=no_of_entries)
        doc = fromstring(tostring(filings_html))

        # Loop through filing index links
        for link in doc.xpath('//a[@href]'):
            href = link.get("href")
            if href and "index.htm" in href:
                index_url = urljoin(BASE_URL, href)
                download_documents_from_index(index_url, output_dir)

        logging.info(f"\nDone downloading for {symbol}. Saved to: {output_dir}")
    except Exception as e:
        logging.info(f"Failed for {symbol}: {e}")


def download_documents_from_index(index_url: str, output_dir: str):
    """Parses the index URL and downloads .htm documents listed in its table."""
    try:
        r = requests.get(index_url, headers={"User-Agent": EDGAR_USER_AGENT})
        r.raise_for_status()
        doc = html.fromstring(r.content)
    except Exception as e:
        logging.info(f"Failed to process index page: {index_url} — {e}")
        return

    for link in doc.xpath('//table[@class="tableFile"]//a'):
        href = link.get("href")

        if href and EDGAR_DOC_PATTERN.match(href):
            full_url = urljoin(BASE_URL, href)
            download_single_htm_file(full_url, output_dir)


def download_single_htm_file(full_url: str, output_dir: str):
    """Downloads and saves a single .htm file from EDGAR."""
    parsed = urlparse(full_url)
    filename = parsed.path.strip("/").replace("/", "_")
    filename_txt = os.path.splitext(filename)[0] + ".txt"
    local_path = os.path.join(output_dir, filename_txt)

    if os.path.exists(local_path):
        logging.info(f"{full_url} EXISTS, SKIPPING..")
        return

    try:
        r = requests.get(full_url, headers={"User-Agent": EDGAR_USER_AGENT})
        r.raise_for_status()
        truncated_content: str = extract_text_before_marker(r.text)

        doc = html.fromstring(truncated_content)
        text_content = doc.text_content()

        with open(local_path, "w", encoding="utf-8") as f:
            f.write(text_content)
        logging.info(f"Saved: {local_path}")
    except Exception as e:
        logging.info(f"Failed to download {full_url} — {e}")

def extract_text_before_marker(raw_filing_text: str, marker_pattern: str = r"<!-- Field: Page; Sequence: \d+ -->") -> str:
    """
    Extracts HTML content inside <TEXT>...</TEXT> tags, but truncates before the marker comment.

    Args:
        raw_filing_text: The full raw filing text containing <TEXT> ... </TEXT>
        marker_pattern: regex pattern that matches the marker comment to stop at.

    Returns:
        The HTML string inside <TEXT> truncated before the marker.
    """

    # Extract full TEXT content first
    text_match = re.search(r"<TEXT>(.*?)</TEXT>", raw_filing_text, re.DOTALL | re.IGNORECASE)
    if not text_match:
        # No closing </TEXT> tag: fallback - everything after <TEXT>
        start_match = re.search(r"<TEXT>(.*)", raw_filing_text, re.DOTALL | re.IGNORECASE)
        if not start_match:
            return ""
        text_content = start_match.group(1)
    else:
        text_content = text_match.group(1)

    # Find the marker position inside the TEXT content
    marker_match = re.search(marker_pattern, text_content, re.IGNORECASE)
    if marker_match:
        # Cut off everything from marker on
        return text_content[:marker_match.start()].strip()
    else:
        # Marker not found - return full TEXT content
        return text_content.strip()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,  # Enable INFO level and above
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    download_htm_documents_for_symbol("CUPR")