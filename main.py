import requests
import pandas as pd
import os
import time
from datetime import datetime

# --- CONFIGURATION ---
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
TRACKING_FILE = "kalshi_hourly_tracker.csv"
FEE_CENTS = 2
MIN_LIQUIDITY_BID = 90

def polite_request(url, params=None):
    try:
        r = requests.get(url, params=params, headers={"User-Agent": "GitHubActionBot/1.0"})
        if r.status_code == 200: 
            return r.json()
        elif r.status_code == 429:
            print("  [!] Rate limit. Sleeping 1s...")
            time.sleep(1)
            return polite_request(url, params)
        else:
            print(f"  [!] API Error {r.status_code}: {r.text[:100]}")
    except Exception as e:
        print(f"Connection Error: {e}")
    return None

def fetch_all_active_markets():
    all_markets = []
    cursor = None
    page_count = 0
    
    print("  -> Fetching market pages...")
    
    while True:
        # FIX: Use 'open' instead of 'active' for the filter
        params = {"limit": 100, "status": "open"}
        if cursor:
            params['cursor'] = cursor
            
        data = polite_request(f"{BASE_URL}/markets", params)
        
        if not data or 'markets' not in data:
            break
            
        markets = data['markets']
        if not markets:
            break
            
        all_markets.extend(markets)
        page_count += 1
        
        # Diagnostic: Print progress so we know it's working
        print(f"     Page {page_count}: Found {len(markets)} markets...")
        
        cursor = data.get('cursor')
        if not cursor:
            break
            
        time.sleep(0.2) 
        
    print(f"  -> Scanned {page_count} pages. Total markets found: {len(all_markets)}")
    return all_markets

def run_hourly_cycle():
    print(f"--- ⏳ Starting Deep Scan: {datetime.utcnow()} UTC ---")
    
    # 1. LOAD CSV
    if os.path.exists(TRACKING_FILE):
        df = pd.read_csv(TRACKING_FILE)
    else:
        df = pd.DataFrame(columns=['ticker', 'question', 'fav_side', 'entry_cost', 
                                   'status', 'open_date', 'close_date', 'result', 'pnl'])

    # 2. RESOLVE PENDING
    pending_mask = df['status'] == 'PENDING'
    if pending_mask.any():
        print(f"Checking {pending_mask.sum()} pending markets...")
        for index, row in df[pending_mask].iterrows():
            ticker = row['ticker']
            url = f"{BASE_URL}/markets/{ticker}"
            data = polite_request(url)
            
            if data:
                m = data.get('market', {})
                if m.get('status') == 'settled':
                    result = m.get('result')
                    if result in ['yes', 'no']:
                        winner = result.upper()
                        did_win = (winner == row['fav_side'])
                        pnl = (100 - row['entry_cost']) - FEE_CENTS if did_win else -row['entry_cost'] - FEE_CENTS
                        
                        df.at[index, 'status'] = 'SETTLED'
                        df.at[index, 'result'] = winner
                        df.at[index, 'pnl'] = pnl
                        print(f"  -> Settled {ticker}: {winner} (PnL: {pnl}¢)")

    # 3. SCAN ALL MARKETS
    markets = fetch_all_active_markets()
    
    new_rows = []
    now = datetime.utcnow()
    
    for m in markets:
        ticker = m['ticker']
        if ticker in df['ticker'].values: continue
        
        yes_bid = m.get('yes_bid', 0)
        yes_ask = m.get('yes_ask', 0)
        
        fav = ""
        cost = 0
        
        if yes_bid >= MIN_LIQUIDITY_BID:
            fav = "YES"
            cost = yes_ask
        elif yes_bid <= (100 - MIN_LIQUIDITY_BID):
            fav = "NO"
            cost = 100 - yes_bid 
        else:
            continue 
        
        if cost < 90: continue
        
        close_str = m.get('close_time')
        close_date = pd.to_datetime(close_str).replace(tzinfo=None) if close_str else "Unknown"

        print(f"  [+] Tracking: {m['title'][:40]}... ({fav} @ {cost}¢)")
        new_rows.append({
            'ticker': ticker,
            'question': m['title'],
            'fav_side': fav,
            'entry_cost': cost,
            'status': 'PENDING',
            'open_date': now,
            'close_date': close_date,
            'result': '',
            'pnl': 0
        })

    # 4. SAVE
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        print(f"  -> Added {len(new_rows)} new markets.")
    else:
        print("  -> No new favorites found in this scan.")
        
    df.to_csv(TRACKING_FILE, index=False)
    print("Cycle Complete. CSV updated.")

if __name__ == "__main__":
    run_hourly_cycle()
