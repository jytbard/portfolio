from intuitlib.client import AuthClient
from quickbooks import QuickBooks
import requests
import pandas as pd
from sqlalchemy import create_engine
import openai
from docx import Document

openai.api_key = '****'


auth_client = AuthClient(
    client_id='****',
    client_secret='****',
    environment='production', 
    redirect_uri='****',
)

auth_url = auth_client.get_authorization_url()

auth_token = auth_client.get_bearer_token(auth_code)

qb_client = QuickBooks(
    sandbox=True,
    client_id='****',
    client_secret='****',
    access_token='****',
    realm_id='****',
)

income_statement = qb_client.reports.get_report('ProfitAndLoss')
balance_sheet = qb_client.reports.get_report('BalanceSheet')
cash_flow = qb_client.reports.get_report('CashFlow')

income_df = pd.DataFrame(income_statement['Rows'])
balance_df = pd.DataFrame(balance_sheet['Rows'])
cashflow_df = pd.DataFrame(cash_flow['Rows'])

engine = create_engine('postgresql://****:****@host:port/****')

# Save DataFrames into the database
income_df.to_sql('income_statement', engine, if_exists='replace')
balance_df.to_sql('balance_sheet', engine, if_exists='replace')
cashflow_df.to_sql('cash_flow', engine, if_exists='replace')

def generate_memo(income_df, balance_df, cashflow_df):
    prompt = f"Using the following financial data, generate an investment memo:\n\n"
    prompt += f"Income Statement: {income_df.to_string()}\n"
    prompt += f"Balance Sheet: {balance_df.to_string()}\n"
    prompt += f"Cash Flow: {cashflow_df.to_string()}\n"

    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=2000
    )

    return response.choices[0].text

memo_text = generate_memo(income_df, balance_df, cashflow_df)

def save_memo_as_docx(memo_text, filename='investment_memo.docx'):
    doc = Document()
    doc.add_paragraph(memo_text)
    doc.save(filename)

save_memo_as_docx(memo_text)
