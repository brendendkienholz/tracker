import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
TRACKING_FILE = "kalshi_hourly_tracker.csv"   # Your Main Dashboard
HISTORY_FILE = "odds_history.csv"             # Your Data Feed
FEE_CENTS = 2
MIN_PROBABILITY = 80
MAX_PROBABILITY = 97
HOURS_AHEAD = 48

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
    print("  -> Fetching market pages...")
    while True:
        params = {"limit": 100, "status": "open"} 
        if cursor: params['cursor'] = cursor
        data = polite_request(f"{BASE_URL}/markets", params)
        if not data or 'markets' not in data: break
        markets = data['markets']
        if not markets: break
        all_markets.extend(markets)
        page_count += 1
        print(f"     Page {page_count}: Found {len(markets)} markets...")
        cursor = data.get('cursor')
        if not cursor: break
        time.sleep(0.2) 
    return all_markets

def run_hourly_cycle():
    print(f"--- ⏳ Starting Live Dashboard Update: {datetime.utcnow()} UTC ---")
    
    # 1. LOAD OR CREATE FILES
    if os.path.exists(TRACKING_FILE):
        df_tracker = pd.read_csv(TRACKING_FILE)
        # Ensure new column exists if updating from old version
        if 'current_price' not in df_tracker.columns:
            df_tracker['current_price'] = df_tracker['entry_cost']
    else:
        df_tracker = pd.DataFrame(columns=['ticker', 'question', 'fav_side', 'entry_cost', 'current_price',
                                           'status', 'open_date', 'close_date', 'result', 'pnl'])
    
    if os.path.exists(HISTORY_FILE):
        df_history = pd.read_csv(HISTORY_FILE)
    else:
        df_history = pd.DataFrame(columns=['timestamp', 'ticker', 'yes_bid', 'yes_ask', 'status'])

    # 2. UPDATE PENDING POSITIONS (Spy Mode + Live Prices)
    pending_mask = df_tracker['status'] == 'PENDING'
    new_history_rows = []
    now = datetime.utcnow()

    if pending_mask.any():
        print(f"Updating {pending_mask.sum()} active positions...")
        
        for index, row in df_tracker[pending_mask].iterrows():
            ticker = row['ticker']
            url = f"{BASE_URL}/markets/{ticker}"
            data = polite_request(url)
            
            if data:
                m = data.get('market', {})
                status = m.get('status')
                
                # --- A. SPY MODE (Print Status) ---
                print(f"   > {ticker} ({row['fav_side']}): Status is '{status}'")
                
                # --- B. CAPTURE DATA ---
                yes_bid = m.get('yes_bid', 0)
                yes_ask = m.get('yes_ask', 0)
                
                # Calculate current market price for YOUR side
                current_price = 0
                if row['fav_side'] == 'YES':
                    # Value is what you could sell it for (Bid)
                    current_price = yes_bid
                else:
                    # Value is 100 - what you could buy Yes for? 
                    # Simpler: If you hold NO, value is 100 - yes_ask (approx)
                    # Let's stick to implied probability:
                    current_price = 100 - yes_ask
                
                # --- C. UPDATE DASHBOARD (Live Price) ---
                df_tracker.at[index, 'current_price'] = current_price
                
                # --- D. LOG HISTORY ---
                new_history_rows.append({
                    'timestamp': now,
                    'ticker': ticker,
                    'yes_bid': yes_bid,
                    'yes_ask': yes_ask,
                    'status': status
                })
                
                # --- E. CHECK SETTLEMENT ---
                if status == 'settled':
                    result = m.get('result')
                    if result in ['yes', 'no']:
                        winner = result.upper()
                        did_win = (winner == row['fav_side'])
                        pnl = (100 - row['entry_cost']) - FEE_CENTS if did_win else -row['entry_cost'] - FEE_CENTS
                        
                        df_tracker.at[index, 'status'] = 'SETTLED'
                        df_tracker.at[index, 'result'] = winner
                        df_tracker.at[index, 'pnl'] = pnl
                        print(f"     🎉 SETTLED: {winner} (PnL: {pnl}¢)")

    # 3. SCAN FOR NEW OPPORTUNITIES
    markets = fetch_all_active_markets()
    new_tracker_rows = []
    time_limit = now + timedelta(hours=HOURS_AHEAD)
    
    # Get list of tickers we are already tracking to avoid duplicates
    tracked_tickers = df_tracker['ticker'].values.tolist()

    for m in markets:
        ticker = m['ticker']
        if ticker in tracked_tickers: continue

        # Time Filter
        close_str = m.get('close_time')
        if not close_str: continue
        try:
            close_date = pd.to_datetime(close_str).replace(tzinfo=None)
            if close_date > time_limit or close_date < now: continue
        except:
            continue

        # Price Filter
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
        
        if cost < MIN_PROBABILITY or cost > MAX_PROBABILITY: 
            continue

        print(f"  [+] New Trade Found: {m['title'][:40]}... ({fav} @ {cost}¢)")
        
        # Add to Dashboard
        new_tracker_rows.append({
            'ticker': ticker,
            'question': m['title'],
            'fav_side': fav,
            'entry_cost': cost,
            'current_price': cost,  # Starts at entry price
            'status': 'PENDING',
            'open_date': now,
            'close_date': close_date,
            'result': '',
            'pnl': 0
        })
        
        # Log History for new item too
        new_history_rows.append({
            'timestamp': now,
            'ticker': ticker,
            'yes_bid': yes_bid,
            'yes_ask': yes_ask,
            'status': 'active'
        })

    # 4. SAVE FILES
    if new_tracker_rows:
        df_tracker = pd.concat([df_tracker, pd.DataFrame(new_tracker_rows)], ignore_index=True)
        print(f"  -> Added {len(new_tracker_rows)} new trades.")
    
    # Save Tracker
    df_tracker.to_csv(TRACKING_FILE, index=False)
    
    # Save History
    if new_history_rows:
        df_history = pd.concat([df_history, pd.DataFrame(new_history_rows)], ignore_index=True)
        print(f"  -> Logged {len(new_history_rows)} data points.")
    
    df_history.to_csv(HISTORY_FILE, index=False)
    print("Cycle Complete. Dashboard updated.")

if __name__ == "__main__":
    run_hourly_cycle()
