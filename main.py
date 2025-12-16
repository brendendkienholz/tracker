def run_hourly_cycle():
    print(f"--- ⏳ Starting Live Dashboard Update: {datetime.utcnow()} UTC ---")
    
    # 1. LOAD FILES
    if os.path.exists(TRACKING_FILE):
        df_tracker = pd.read_csv(TRACKING_FILE)
        if 'current_price' not in df_tracker.columns:
            df_tracker['current_price'] = df_tracker['entry_cost']
    else:
        df_tracker = pd.DataFrame(columns=['ticker', 'question', 'fav_side', 'entry_cost', 'current_price',
                                           'status', 'open_date', 'close_date', 'result', 'pnl'])
    
    if os.path.exists(HISTORY_FILE):
        df_history = pd.read_csv(HISTORY_FILE)
    else:
        df_history = pd.DataFrame(columns=['timestamp', 'ticker', 'yes_bid', 'yes_ask', 'status'])

    # 2. UPDATE POSITIONS & CALCULATE PAPER PROFIT
    pending_mask = df_tracker['status'] == 'PENDING'
    new_history_rows = []
    now = datetime.utcnow()
    
    # Trackers for "Paper Profit"
    total_unrealized_pnl = 0
    active_positions = 0

    if pending_mask.any():
        print(f"Updating {pending_mask.sum()} active positions...")
        
        for index, row in df_tracker[pending_mask].iterrows():
            ticker = row['ticker']
            url = f"{BASE_URL}/markets/{ticker}"
            data = polite_request(url)
            
            if data:
                m = data.get('market', {})
                status = m.get('status')
                yes_bid = m.get('yes_bid', 0)
                yes_ask = m.get('yes_ask', 0)
                
                # --- CALCULATE CURRENT VALUE ---
                current_price = 0
                if row['fav_side'] == 'YES':
                    current_price = yes_bid
                else:
                    # If we have NO, value is roughly (100 - yes_ask)
                    current_price = 100 - yes_ask
                
                # Update DataFrame
                df_tracker.at[index, 'current_price'] = current_price
                
                # Calculate Paper PnL for this specific trade
                # (Current Value - Entry Cost)
                paper_pnl = current_price - row['entry_cost']
                total_unrealized_pnl += paper_pnl
                active_positions += 1
                
                # Log Status with Visual Cues
                status_icon = "🟢" if paper_pnl > 0 else ("🔴" if paper_pnl < 0 else "⚪")
                print(f"   > {ticker}: {status_icon} PnL: {paper_pnl:+d}¢ (Price: {current_price}¢) [{status}]")

                # Log History
                new_history_rows.append({
                    'timestamp': now,
                    'ticker': ticker,
                    'yes_bid': yes_bid,
                    'yes_ask': yes_ask,
                    'status': status
                })
                
                # Check for Settlement
                if status == 'settled':
                    result = m.get('result')
                    if result in ['yes', 'no']:
                        winner = result.upper()
                        did_win = (winner == row['fav_side'])
                        pnl = (100 - row['entry_cost']) - FEE_CENTS if did_win else -row['entry_cost'] - FEE_CENTS
                        
                        df_tracker.at[index, 'status'] = 'SETTLED'
                        df_tracker.at[index, 'result'] = winner
                        df_tracker.at[index, 'pnl'] = pnl
                        print(f"     🎉 SETTLED: {winner} (Realized PnL: {pnl}¢)")

    # --- PRINT SUMMARY ---
    if active_positions > 0:
        print(f"   ------------------------------------------------")
        print(f"   💰 TOTAL PAPER PROFIT: {total_unrealized_pnl:+d}¢ over {active_positions} trades")
        print(f"   ------------------------------------------------")

    # 3. SCAN FOR NEW OPPORTUNITIES (Standard logic below...)
    markets = fetch_all_active_markets()
    new_tracker_rows = []
    time_limit = now + timedelta(hours=HOURS_AHEAD)
    tracked_tickers = df_tracker['ticker'].values.tolist()

    for m in markets:
        ticker = m['ticker']
        if ticker in tracked_tickers: continue

        close_str = m.get('close_time')
        if not close_str: continue
        try:
            close_date = pd.to_datetime(close_str).replace(tzinfo=None)
            if close_date > time_limit or close_date < now: continue
        except: continue

        yes_bid = m.get('yes_bid', 0)
        yes_ask = m.get('yes_ask', 0)
        
        fav = ""
        cost = 0
        if yes_bid >= MIN_PROBABILITY:
            fav = "YES"; cost = yes_ask
        elif yes_bid <= (100 - MIN_PROBABILITY):
            fav = "NO"; cost = 100 - yes_bid 
        else: continue 
        
        if cost < MIN_PROBABILITY or cost > MAX_PROBABILITY: continue

        print(f"  [+] New Trade Found: {m['title'][:40]}... ({fav} @ {cost}¢)")
        new_tracker_rows.append({
            'ticker': ticker, 'question': m['title'], 'fav_side': fav,
            'entry_cost': cost, 'current_price': cost, 'status': 'PENDING',
            'open_date': now, 'close_date': close_date, 'result': '', 'pnl': 0
        })
        new_history_rows.append({'timestamp': now, 'ticker': ticker, 'yes_bid': yes_bid, 'yes_ask': yes_ask, 'status': 'active'})

    # 4. SAVE
    if new_tracker_rows:
        df_tracker = pd.concat([df_tracker, pd.DataFrame(new_tracker_rows)], ignore_index=True)
    
    df_tracker.to_csv(TRACKING_FILE, index=False)
    
    if new_history_rows:
        df_history = pd.concat([df_history, pd.DataFrame(new_history_rows)], ignore_index=True)
    df_history.to_csv(HISTORY_FILE, index=False)
    print("Cycle Complete.")
