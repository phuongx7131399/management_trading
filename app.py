import os
from datetime import datetime
from supabase import create_client, Client
from pycoingecko import CoinGeckoAPI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import pandas as pd
import numpy as np

# Load environment variables
load_dotenv()

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# CoinGecko API setup
cg = CoinGeckoAPI()

# Token ID mapping for CoinGecko
TOKEN_MAPPING = {
    'eth': 'ethereum',
    'ethereum': 'ethereum',  # Match token_id in the table
    # Add more mappings if needed, e.g., 'btc': 'bitcoin'
}

# Google Sheets setup
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_NAME)

# Function to get current price from CoinGecko
def get_current_price(token_id):
    try:
        coingecko_id = TOKEN_MAPPING.get(token_id.lower(), token_id)
        price_data = cg.get_price(ids=coingecko_id, vs_currencies='usd')
        if coingecko_id not in price_data:
            raise ValueError(f"No price data found for {coingecko_id}")
        return price_data[coingecko_id]['usd']
    except Exception as e:
        print(f"Error fetching price for {token_id} (mapped to {coingecko_id}): {e}")
        return None

# Function to update present_price in Supabase
def update_present_price():
    try:
        response = supabase.table("trading_table").select("*").execute()
        data = response.data
        print(f"Fetched {len(data)} rows from trading_table")
        
        for row in data:
            token_id = row['token_id']
            current_price = get_current_price(token_id)
            if current_price:
                supabase.table("trading_table").update({
                    "present_price": current_price
                }).eq("id", row['id']).execute()
                print(f"Updated present_price for {token_id}: {current_price}")
            else:
                print(f"Skipped updating present_price for {token_id} due to API error")
    except Exception as e:
        print(f"Error updating present_price: {e}")

# Function to calculate average buy price per trading_id
def calculate_average_buy_price(trading_id, status_filter):
    try:
        response = supabase.table("trading_table").select("*").eq("trading_id", trading_id).eq("trading_status", status_filter).execute()
        data = response.data
        print(f"Calculating average buy price for trading_id={trading_id}, status={status_filter}: {len(data)} rows")
        
        if not data:
            return None, 0
        
        total_amount = sum(float(row['amount']) for row in data if row['amount'] is not None)
        weighted_price = sum(float(row['amount']) * float(row['price']) for row in data if row['amount'] is not None and row['price'] is not None)
        avg_price = weighted_price / total_amount if total_amount > 0 else 0
        return avg_price, total_amount
    except Exception as e:
        print(f"Error calculating average buy price for {trading_id}: {e}")
        return None, 0

# Function to calculate PNL
def calculate_pnl():
    try:
        response = supabase.table("trading_table").select("trading_id").execute()
        trading_ids = list(set(row['trading_id'] for row in response.data if row['trading_id'] is not None))
        print(f"Found trading_ids: {trading_ids}")
        
        results = []
        for trading_id in trading_ids:
            # Calculate PNL for 'process' status
            avg_price_process, total_amount_process = calculate_average_buy_price(trading_id, "process")
            print(f"Process status for trading_id={trading_id}: avg_price={avg_price_process}, amount={total_amount_process}")
            if avg_price_process is not None:
                response = supabase.table("trading_table").select("present_price, token_id, created_at").eq("trading_id", trading_id).eq("trading_status", "process").limit(1).execute()
                if response.data and response.data[0]['present_price'] is not None:
                    present_price = float(response.data[0]['present_price'])
                    token_id = response.data[0]['token_id']
                    created_at = response.data[0]['created_at']
                    unrealized_pnl = (present_price - avg_price_process) * total_amount_process
                    results.append({
                        "date": created_at,
                        "token_name": token_id,
                        "avg_buy_price": avg_price_process,
                        "amount": total_amount_process,
                        "unrealized_pnl": unrealized_pnl,
                        "realized_pnl": 0,
                        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    print(f"Added process PNL for {trading_id}: {results[-1]}")
            
            # Calculate PNL for 'done' status
            avg_price_done, total_amount_done = calculate_average_buy_price(trading_id, "done")
            print(f"Done status for trading_id={trading_id}: avg_price={avg_price_done}, amount={total_amount_done}")
            if avg_price_done is not None:
                response = supabase.table("trading_table").select("present_price, token_id, created_at").eq("trading_id", trading_id).eq("trading_status", "done").limit(1).execute()
                if response.data and response.data[0]['present_price'] is not None:
                    present_price = float(response.data[0]['present_price'])
                    token_id = response.data[0]['token_id']
                    created_at = response.data[0]['created_at']
                    realized_pnl = (present_price - avg_price_done) * total_amount_done
                    results.append({
                        "date": created_at,
                        "token_name": token_id,
                        "avg_buy_price": avg_price_done,
                        "amount": total_amount_done,
                        "unrealized_pnl": 0,
                        "realized_pnl": realized_pnl,
                        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    print(f"Added done PNL for {trading_id}: {results[-1]}")
        
        return results
    except Exception as e:
        print(f"Error calculating PNL: {e}")
        return []

# Function to push data to Google Sheets
def push_to_google_sheets(data):
    try:
        headers = ["date", "token_name", "avg_buy_price", "amount", "unrealized_pnl", "realized_pnl", "last_update"]
        if not data:
            print("No data to push to Google Sheets")
            df = pd.DataFrame(columns=headers)
        else:
            df = pd.DataFrame(data, columns=headers)
            df = df.fillna(0)  # Replace NaN with 0
            print("Data to be pushed to Google Sheets:", df.to_dict(orient='records'))
        
        sheet.clear()
        sheet.update(values=[headers], range_name="A1")
        sheet.update(values=df.values.tolist(), range_name="A2")
        print("Data successfully pushed to Google Sheets")
    except Exception as e:
        print(f"Error pushing data to Google Sheets: {e}")

# Main execution
if __name__ == "__main__":
    update_present_price()
    pnl_data = calculate_pnl()
    push_to_google_sheets(pnl_data)
