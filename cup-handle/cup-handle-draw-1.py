#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import alpaca_trade_api as tradeapi
import os
from dotenv import load_dotenv
import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt

# --- Configuration ---
TICKERS_FILE = "tickers.csv"
TIMEFRAME = '4H' 
RATE_LIMIT_DELAY = 1.0 # seconds

# --- Pattern Detection Parameters ---
CUP_MIN_DURATION = 12
CUP_MAX_DURATION = 48
CUP_DEPTH_PCT = 0.20
HANDLE_DURATION = 8
HANDLE_MAX_PULLBACK_PCT = 0.10

# --- Setup Alpaca API ---
load_dotenv("alpkey.env")
ALPACA_API_KEY = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise ValueError("Alpaca API keys are not set.")

api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, BASE_URL, api_version="v2")
_last_request_time = 0.0

def rate_limited_get_bars(api, symbol, timeframe, start, end, limit=None, adjustment='all', feed='iex'):
    global _last_request_time
    time_since_last_request = time.time() - _last_request_time
    if time_since_last_request < RATE_LIMIT_DELAY:
        time.sleep(RATE_LIMIT_DELAY - time_since_last_request)
    barset = api.get_bars(symbol, timeframe, start=start, end=end, limit=limit, adjustment=adjustment, feed=feed)
    _last_request_time = time.time()
    return barset

def is_cup_and_handle(df, symbol):
    # Ensure enough data
    if len(df) < CUP_MIN_DURATION + HANDLE_DURATION:
        return False
    
    handle_start_idx = len(df) - HANDLE_DURATION
    handle_period = df.iloc[handle_start_idx:]
    cup_period = df.iloc[:handle_start_idx]
    
    if len(cup_period) < CUP_MIN_DURATION:
        return False

    right_lip_price = cup_period['close'].iloc[-1]
    handle_low_price = handle_period['close'].min()
    if (right_lip_price - handle_low_price) / right_lip_price > HANDLE_MAX_PULLBACK_PCT:
        return False
    
    cup_low_price = cup_period['close'].min()
    cup_high_price = cup_period['close'].max()
    if (cup_high_price - cup_low_price) / cup_high_price < CUP_DEPTH_PCT:
        return False
    
    current_price = df.iloc[-1]['close']
    if current_price < right_lip_price * (1 - HANDLE_MAX_PULLBACK_PCT):
        return False

    left_lip_price = cup_period['close'].iloc[0]
    if abs(right_lip_price - left_lip_price) / right_lip_price > 0.05:
        return False

    return True

def plot_cup_handle(df, symbol):
    """
    Draws and saves the Cup & Handle pattern chart as PNG
    """
    handle_start_idx = len(df) - HANDLE_DURATION
    cup_period = df.iloc[:handle_start_idx]
    handle_period = df.iloc[handle_start_idx:]

    left_lip = cup_period['close'].iloc[0]
    right_lip = cup_period['close'].iloc[-1]
    cup_low = cup_period['close'].min()
    bottom_idx = cup_period['close'].idxmin()
    current_price = df.iloc[-1]['close']

    plt.figure(figsize=(10,6))
    plt.plot(df.index, df['close'], label='Close', color='blue')
    plt.plot(cup_period.index, cup_period['close'], label='Cup', color='green', linewidth=2)
    plt.plot(handle_period.index, handle_period['close'], label='Handle', color='orange', linewidth=2)
    plt.scatter(cup_period.index[0], left_lip, color='red', s=50, label='Left Lip')
    plt.scatter(cup_period.index[-1], right_lip, color='red', s=50, label='Right Lip')
    plt.scatter(bottom_idx, cup_low, color='purple', s=50, label='Cup Bottom')
    plt.scatter(df.index[-1], current_price, color='black', s=50, label='Current Close')

    plt.title(f"Cup & Handle Pattern: {symbol}")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    out_dir = "plots"
    os.makedirs(out_dir, exist_ok=True)
    plt.savefig(os.path.join(out_dir, f"{symbol}_cup_handle.png"))
    plt.close()

def main():
    try:
        stocks_df = pd.read_csv(TICKERS_FILE)
        symbol_col = next(col for col in ['Symbol','symbol','Ticker','Tickers'] if col in stocks_df.columns)
        tickers = stocks_df[symbol_col].tolist()
        print(f"Loaded {len(tickers)} tickers.")
    except Exception as e:
        print(f"Error loading tickers: {e}")
        return

    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    cup_and_handle_stocks = []

    for ticker in tickers:
        try:
            barset = rate_limited_get_bars(api, ticker, TIMEFRAME,
                                           start=start_date.date().isoformat(),
                                           end=end_date.date().isoformat(), limit=1000)
            df = barset.df
            if df is not None and not df.empty:
                if is_cup_and_handle(df, ticker):
                    cup_and_handle_stocks.append(ticker)
                    plot_cup_handle(df, ticker)
            else:
                print(f"No data for {ticker}")
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            continue

    if cup_and_handle_stocks:
        print("\n--- Cup & Handle candidates ---")
        for s in cup_and_handle_stocks:
            print(f"- {s}")
    else:
        print("\nNo Cup & Handle patterns detected.")

if __name__ == "__main__":
    main()
