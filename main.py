import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
TRACKING_FILE = "kalshi_hourly_tracker.csv"
FEE_CENTS = 2  # Est. taker fee per contract

def polite_request(url, params=None):
    try:
        r = requests.get(url, params=params, headers={"User-Agent": "GitHubActionBot/1.0"})
        if r.status_code == 200: return r.json()
    except Exception as e:
        print(f"Connection Error: {e}")
    return None

def run_hourly_cycle():
    print(f"--- ⏳ Starting Hourly Scan: {datetime.utcnow()} UTC ---")
    
    # 1. LOAD EXISTING DATA
    if os.path.exists(TRACKING_FILE):
        df = pd.read_csv(TRACKING_FILE)
    else:
        # Initialize with columns if file doesn't exist
        df = pd.DataFrame(columns=['ticker', 'question', 'fav_side', 'entry_cost', 
                                   'status', 'open_date', 'close_date', 'result', 'pnl'])

    # 2. RESOLVE PENDING MARKETS (Check if yesterday's bets settled)
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
                    result = m.get('result') # 'yes' or 'no'
                    if result in ['yes', 'no']:
                        winner = result.upper()
                        did_win = (winner == row['fav_side'])
                        pnl = (100 - row['entry_cost']) - FEE_CENTS if did_win else -row['entry_cost'] - FEE_CENTS
                        
                        df.at[index, 'status'] = 'SETTLED'
                        df.at[index, 'result'] = winner
                        df.at[index, 'pnl'] = pnl
                        print(f"  -> Settled {ticker}: {winner} (PnL: {pnl}¢)")

    # 3. SCAN FOR NEW OPPORTUNITIES (Closing in < 24h)
    url = f"{BASE_URL}/markets"
    params = {"limit": 100, "status": "active"}
    data = polite_request(url, params)
    
    new_rows = []
    if data:
        now = datetime.utcnow()
        tomorrow = now + timedelta(hours=24)
        
        for m in data.get('markets', []):
            # Filter: Must close soon & haven't tracked it yet
            if m['ticker'] in df['ticker'].values: continue
            
            close_str = m.get('close_time')
            if not close_str: continue
            close_date = pd.to_datetime(close_str).replace(tzinfo=None)
            
            if close_date > tomorrow or close_date < now: continue
            
            # Logic: Find Heavy Favorites (>90 cents)
            yes_bid = m.get('yes_bid', 0)
            
            if yes_bid >= 90:
                fav = "YES"; cost = m['yes_ask']
            elif yes_bid <= 10:
                fav = "NO"; cost = 100 - yes_bid
            else:
                continue # Not a favorite
            
            if cost < 90: continue # Spread too wide or price dropped
            
            print(f"  [+] Tracking: {m['title'][:30]}... ({fav} @ {cost}¢)")
            new_rows.append({
                'ticker': m['ticker'],
                'question': m['title'],
                'fav_side': fav,
                'entry_cost': cost,
                'status': 'PENDING',
                'open_date': now,
                'close_date': close_date,
                'result': '',
                'pnl': 0
            })

    # 4. SAVE & EXIT
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    
    df.to_csv(TRACKING_FILE, index=False)
    print("Cycle Complete. CSV updated.")

if __name__ == "__main__":
    run_hourly_cycle()
