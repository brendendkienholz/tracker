import requests
import pandas as pd
import os
import time
from datetime import datetime

# --- CONFIGURATION ---
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
DATA_FOLDER = "market_data"

def polite_request(url, params=None):
    try:
        r = requests.get(url, params=params, headers={"User-Agent": "GitHubActionBot/1.0"}, timeout=10)
        if r.status_code == 200: 
            return r.json()
        elif r.status_code == 429:
            print("  [!] Rate limit. Sleeping 1s...")
            time.sleep(1)
            return polite_request(url, params)
    except Exception as e:
        print(f"Connection Error: {e}")
    return None

def fetch_all_active_markets():
    all_markets = []
    cursor = None
    page_count = 0
    retry_count = 0
    
    print("  -> Fetching market pages...")
    while True:
        params = {"limit": 100, "status": "open"}
        if cursor: params['cursor'] = cursor
        
        data = polite_request(f"{BASE_URL}/markets", params)
        
        if not data:
            retry_count += 1
            if retry_count > 3: break
            print(f"  [!] Retrying (Attempt {retry_count}/3)...")
            time.sleep(2)
            continue
        
        retry_count = 0
        if 'markets' not in data: break
        markets = data['markets']
        if not markets: break
        
        all_markets.extend(markets)
        page_count += 1
        print(f"     Page {page_count}: Found {len(markets)} markets...")
        
        cursor = data.get('cursor')
        if not cursor: break
        time.sleep(0.2) 
        
    print(f"  -> Scanned {page_count} pages. Total candidates: {len(all_markets)}")
    return all_markets

def run_binary_hoarder():
    now = datetime.utcnow()
    print(f"--- 🚜 Starting Binary Data Hoard (v3): {now} UTC ---")
    
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        
    markets = fetch_all_active_markets()
    snapshot_rows = []
    
    for m in markets:
        # 1. BINARY FILTER: Must have valid Yes/No prices
        if 'yes_bid' not in m or 'yes_ask' not in m: continue
            
        yes_bid = m.get('yes_bid', 0)
        yes_ask = m.get('yes_ask', 0)
        
        # Skip "Dead" markets (Price 0 or 100) - No signal there for ML
        if yes_bid == 0 and yes_ask == 0: continue

        # 2. FEATURE ENGINEERING
        spread = yes_ask - yes_bid
        midpoint = (yes_ask + yes_bid) / 2
        
        # Calculate Time to Close (Critical for ML)
        close_str = m.get('close_time')
        hours_left = 0
        if close_str:
            try:
                close_dt = datetime.strptime(close_str, "%Y-%m-%dT%H:%M:%SZ")
                delta = close_dt - now
                hours_left = delta.total_seconds() / 3600
            except:
                hours_left = -1

        # Extract Category & Class
        category = m.get('category', 'Uncategorized')
        ticker_parts = m['ticker'].split('-')
        ticker_class = ticker_parts[0] if ticker_parts else 'UNKNOWN'

        snapshot_rows.append({
            'timestamp': now,
            'ticker': m['ticker'],
            'category': category,          # Feature: Market Type (Crypto, Politics)
            'class': ticker_class,         # Feature: Sub-Type (BTC, FED)
            'hours_to_close': round(hours_left, 2), # Feature: Time Decay
            'yes_bid': yes_bid,
            'yes_ask': yes_ask,
            'spread': spread,              # Feature: Liquidity Cost
            'midpoint': midpoint,          # Feature: "True" Price
            'volume': m.get('volume', 0),  # Feature: Activity
            'open_interest': m.get('open_interest', 0), # Feature: Depth
            'close_date': close_str
        })
        
    if not snapshot_rows:
        print("No valid binary markets found.")
        return

    # 3. SAVE TO DAILY FILE
    date_str = now.strftime('%Y-%m-%d')
    filename = f"{DATA_FOLDER}/{date_str}.csv"
    
    df_new = pd.DataFrame(snapshot_rows)
    
    if os.path.exists(filename):
        df_new.to_csv(filename, mode='a', header=False, index=False)
        print(f"  -> Appended {len(df_new)} rows to {filename}")
    else:
        df_new.to_csv(filename, index=False)
        print(f"  -> Created new daily file: {filename}")

if __name__ == "__main__":
    run_binary_hoarder()
