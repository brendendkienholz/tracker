import requests
import pandas as pd
import os
from datetime import datetime

# --- CONFIGURATION ---
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
TRACKING_FILE = "kalshi_hourly_tracker.csv"
FEE_CENTS = 2  # Est. taker fee per contract
MIN_LIQUIDITY_BID = 90 # Filter for favorites priced 90-99 cents

def polite_request(url, params=None):
    try:
        r = requests.get(url, params=params, headers={"User-Agent": "GitHubActionBot/1.0"})
        if r.status_code == 200: return r.json()
    except Exception as e:
        print(f"Connection Error: {e}")
    return None

def run_hourly_cycle():
    print(f"--- ⏳ Starting Unrestricted Scan: {datetime.utcnow()} UTC ---")
    
    # 1. LOAD OR CREATE CSV
    if os.path.exists(TRACKING_FILE):
        df = pd.read_csv(TRACKING_FILE)
    else:
        df = pd.DataFrame(columns=['ticker', 'question', 'fav_side', 'entry_cost', 
                                   'status', 'open_date', 'close_date', 'result', 'pnl'])

    # 2. RESOLVE PENDING MARKETS
    # We check ALL pending markets, regardless of when we added them
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

    # 3. SCAN ALL ACTIVE MARKETS (No Time Filter)
    # Fetch active markets (Kalshi limits response size, so we grab a larger batch)
    url = f"{BASE_URL}/markets"
    params = {"limit": 200, "status": "active"} 
    data = polite_request(url, params)
    
    new_rows = []
    if data:
        now = datetime.utcnow()
        
        for m in data.get('markets', []):
            ticker = m['ticker']
            
            # Skip if we are already tracking this EXACT ticker
            if ticker in df['ticker'].values: continue
            
            # --- THE STRATEGY ---
            # Look for ANY heavy favorite (Yes or No)
            yes_bid = m.get('yes_bid', 0)
            yes_ask = m.get('yes_ask', 0)
            
            # Logic: If Bid is 90+, "YES" is the favorite.
            # If Bid is <10, "NO" is the favorite (implied price > 90)
            
            fav = ""
            cost = 0
            
            if yes_bid >= MIN_LIQUIDITY_BID:
                fav = "YES"
                cost = yes_ask # We buy at the Ask
            elif yes_bid <= (100 - MIN_LIQUIDITY_BID):
                fav = "NO"
                # Cost to buy NO is roughly (100 - yes_bid). 
                # To be conservative/safe, we assume we pay a premium.
                cost = 100 - yes_bid 
            else:
                continue # Not a heavy favorite
            
            # Safety: Ensure the cost is actually > 90 cents (tight spread)
            if cost < 90: continue
            
            # Get closing time for record keeping
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

    # 4. SAVE & EXIT
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        print(f"  -> Added {len(new_rows)} new markets.")
    else:
        print("  -> No new markets found this cycle.")
        
    df.to_csv(TRACKING_FILE, index=False)
    print("Cycle Complete. CSV updated.")

if __name__ == "__main__":
    run_hourly_cycle()
