import yfinance as yf
import datetime
from firebase_admin import credentials, db, initialize_app
from ta.trend import ADXIndicator, IchimokuIndicator
import pandas as pd
import json
import os

# Initialize Firebase
if not db._apps:
    cred = credentials.Certificate("firebase_key.json")
    initialize_app(cred, {
        "databaseURL": os.getenv("FIREBASE_DB_URL")
    })

# List of Nifty 50 stocks
nifty_50 = [
    "RELIANCE", "INFY", "TCS", "HDFCBANK", "SBIN", "ICICIBANK", "ITC", "LT",
    "KOTAKBANK", "HINDUNILVR", "AXISBANK", "BHARTIARTL", "HCLTECH", "ASIANPAINT",
    "MARUTI", "BAJFINANCE", "WIPRO", "HINDALCO", "NTPC", "TECHM", "TITAN",
    "COALINDIA", "TATASTEEL", "ONGC", "SUNPHARMA", "POWERGRID", "GRASIM",
    "ULTRACEMCO", "DRREDDY", "CIPLA", "ADANIENT", "DIVISLAB", "BAJAJFINSV",
    "HEROMOTOCO", "EICHERMOT", "UPL", "BPCL", "JSWSTEEL", "SHREECEM",
    "SBILIFE", "INDUSINDBK", "APOLLOHOSP", "HDFCLIFE", "BRITANNIA",
    "NESTLEIND", "BAJAJ-AUTO", "TATAMOTORS", "M&M", "ICICIPRULI", "DLF"
]

# Track last signals to avoid repeat
last_pushed = {}

# Fetch and calculate for each stock
for symbol in nifty_50:
    ticker = yf.Ticker(f"{symbol}.NS")
    df = ticker.history(period="2mo", interval="1d")
    if len(df) < 52:
        continue

    df.dropna(inplace=True)

    try:
        adx = ADXIndicator(df['High'], df['Low'], df['Close'], window=14)
        df['+DI'] = adx.plus_di()
        df['-DI'] = adx.minus_di()

        ichi = IchimokuIndicator(df['High'], df['Low'], window1=9, window2=26, window3=52)
        df['Span_A'] = ichi.ichimoku_a()
        df['Span_B'] = ichi.ichimoku_b()

        latest = df.iloc[-1]
        di_bull = latest['+DI']
        di_bear = latest['-DI']
        ichi_bull = latest['Span_A']
        ichi_bear = latest['Span_B']

        # Signal logic
        signal = None
        if di_bear >= 40 and ichi_bull >= 26:
            signal = "reversal_bull"
        elif di_bull >= 40 and ichi_bear >= 26:
            signal = "reversal_bear"
        elif di_bull >= 40 and ichi_bull >= 26:
            signal = "breakout_bull"
        elif di_bear >= 40 and ichi_bear >= 26:
            signal = "breakout_bear"

        if signal:
            now = datetime.datetime.now()
            signal_path = f"/signals/{signal}"
            stock_path = f"{signal_path}/{symbol}"

            # Skip if same signal already pushed in last 10 minutes
            if symbol in last_pushed and signal in last_pushed[symbol]:
                last_time = last_pushed[symbol][signal]
                if (now - last_time).total_seconds() < 600:
                    continue

            # Push to Firebase (type-level and history)
            data = {
                "symbol": symbol,
                "signal_type": signal,
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
            }

            db.reference(stock_path).set(data)
            db.reference(f"/signals/history/{signal}").push(data)

            # Update last pushed time
            if symbol not in last_pushed:
                last_pushed[symbol] = {}
            last_pushed[symbol][signal] = now

            print(f"✅ {symbol}: {signal} signal pushed.")

    except Exception as e:
        print(f"❌ Error for {symbol}: {e}")

print("✅ All stocks processed.")
