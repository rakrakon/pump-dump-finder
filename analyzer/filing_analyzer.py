import json
import os

from dotenv import load_dotenv
from openai import OpenAI

from database.chat_storage import get_conversation_by_company, update_conversation_by_id, upsert_conversation
from database.db_setup import connect_db, create_table

import logging

from edgar_files_fetcher import NEW_FILINGS_FILENAME

load_dotenv()
client = OpenAI()
conn = connect_db()

MODEL = "gpt-4o-mini"

SYSTEM_PROMPT = """
You are an expert financial analyst with a PhD in finance and many years of experience in analyzing SEC filings to find pump-and-dump stocks.
Your role is to review the SEC filings of a company and provide the risk level 1-5 in json format that this company is a pump-and-dump stock.
{
  risk: <risk_level>,
  justification_text: "<description of your reasoning in up to 30 words"
}
If we miss opportunities (we prefer false positives), I will lose my job and will need to pay a fine of $10000000
When reasoning about this, use past examples of pump-and-dump stocks and technical indicators.
Each time I'll provide you with a single filing. You should check this filing in context with all the previous filings I've provided.
e.g., potentially the first X filings of a company are at risk level 3 but then the X+1 filing when combined with the previous X will move this to 4 or 5.
"""

NEW_FILINGS_PROMPT = """
Here's an additional filing (date is inside the below text extracted from the sec filing), please act according to the system prompt\n
"""


def add_new_filings_to_chat(company_symbol, new_filings):
    logging.info(f"Loading past conversation for {company_symbol}..")

    company_query = get_conversation_by_company(conn, company_symbol)
    conversation_history = company_query.get("content", []) if company_query else []

    for file in new_filings:
        conversation_history = add_filing_to_chat(file, conversation_history)

    logging.info("Saving conversation history to database..")
    upsert_conversation(conn, company_symbol, conversation_history)


def add_filing_to_chat(new_filing, conversation_history):
    logging.info(f"Analyzing new filing..")

    chat_input = (
            [{"role": "system", "content": SYSTEM_PROMPT}]
            + conversation_history
            + [{"role": "user", "content": new_filing}]
    )

    response = client.responses.create(
        model= MODEL,
        input = chat_input,
    )

    logging.info("Adding response to conversation history..")
    conversation_history += [{"role": el.role, "content": el.content} for el in response.output]

    return conversation_history

def get_new_filings_for_symbol(company_symbol):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_dir = os.path.join(os.path.dirname(current_dir), "data", "edgar_documents", company_symbol)
    json_path = os.path.join(json_dir, NEW_FILINGS_FILENAME)

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data

# Test
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO
    )
    symbol = "CUPR"
    filings = get_new_filings_for_symbol(symbol)
    add_new_filings_to_chat(symbol, filings)