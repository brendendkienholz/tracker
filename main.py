import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
TRACKING_FILE = "kalshi_hourly_tracker.csv"
FEE_CENTS = 2
MIN_PROBABILITY = 80  # 80¢ Floor
MAX_PROBABILITY = 97  # 97¢ Cap
HOURS_AHEAD = 48      # Only track markets closing in next 48h

def polite_request(url, params=None):
    try:
        # TIMEOUT ADDED: Prevents the script from hanging forever
        r = requests.get(url, params=params, headers={"User-Agent": "GitHubActionBot/1.0"}, timeout=10)
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
        
        # Diagnostic print
        print(f"     Page {page_count}: Found {len(markets)} markets...")
        
        cursor = data.get('cursor')
        if not cursor:
            break
            
        time.sleep(0.2) 
        
    print(f"  -> Scanned {page_count} pages. Total markets found: {len(all_markets)}")
    return all_markets

def run_hourly_cycle():
    print(f"--- ⏳ Starting Scan (Next {HOURS_AHEAD}h Only): {datetime.utcnow()} UTC ---")
    
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
                        
                        # PnL Math
                        pnl = (100 - row['entry_cost']) - FEE_CENTS if did_win else -row['entry_cost'] - FEE_CENTS
                        
                        df.at[index, 'status'] = 'SETTLED'
                        df.at[index, 'result'] = winner
                        df.at[index, 'pnl'] = pnl
                        print(f"  -> Settled {ticker}: {winner} (PnL: {pnl}¢)")

    # 3. SCAN FOR NEW TRADES
    markets = fetch_all_active_markets()
    
    new_rows = []
    now = datetime.utcnow()
    # Define the cutoff time (Now + 48 Hours)
    time_limit = now + timedelta(hours=HOURS_AHEAD)
    
    for m in markets:
        ticker = m['ticker']
        if ticker in df['ticker'].values: continue
        
        # --- TIME FILTER ---
        close_str = m.get('close_time')
        if not close_str: continue 
        
        try:
            close_date = pd.to_datetime(close_str).replace(tzinfo=None)
        except:
            continue
            
        # The 48-Hour Check
        if close_date > time_limit:
            continue 
        
        if close_date < now:
            continue 
            
        # --- PRICE FILTER ---
        yes_bid = m.get('yes_bid', 0)
        yes_ask = m.get('yes_ask', 0)
        
        fav = ""
        cost = 0
        
        if yes_bid >= MIN_PROBABILITY:
            fav = "YES"
            cost = yes_ask
        elif yes_bid <= (100 - MIN_PROBABILITY):
            fav = "NO"
            cost = 100 - yes_bid 
        else:
            continue 
        
        # 80-97 Filter
        if cost < MIN_PROBABILITY or cost > MAX_PROBABILITY: 
            continue

        print(f"  [+] Tracking: {m['title'][:40]}... ({fav} @ {cost}¢) | Closes: {close_date.strftime('%m-%d %H:%M')}")
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
        print(f"  -> Added {len(new_rows)} new short-term markets.")
    else:
        print("  -> No new opportunities found in the next 48h.")
        
    df.to_csv(TRACKING_FILE, index=False)
    print("Cycle Complete. CSV updated.")

if __name__ == "__main__":
    run_hourly_cycle()
