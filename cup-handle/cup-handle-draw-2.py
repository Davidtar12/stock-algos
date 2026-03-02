#!/usr/bin/env python3
# coding: utf-8
"""
cup_handle_production_plot.py
Robust Cup & Handle scanner using Alpaca + pandas_ta smoothing.
Includes drawing of detected patterns.
"""

from datetime import datetime, timedelta
import time
import os
import math

import pandas as pd
import numpy as np
import pandas_ta as ta
from dotenv import load_dotenv
import alpaca_trade_api as tradeapi
import matplotlib
matplotlib.use("Agg")  # non-GUI backend, avoids hanging
import matplotlib.pyplot as plt

# Optional progress bar
try:
    from tqdm import tqdm
    _HAS_TQDM = True
except Exception:
    _HAS_TQDM = False

# ------------------ CONFIG ------------------
TICKERS_FILE = "tickers.csv"   # first column expected to be tickers
TIMEFRAME = "4H"               # e.g., "1D", "4H", "1H", "15Min"
RATE_LIMIT_DELAY = 1.0         # seconds between calls

# Cup & Handle parameters (bars, not days)
CUP_MIN_BARS = 12
CUP_MAX_BARS = 48
CUP_MIN_DEPTH = 0.12
CUP_MAX_DEPTH = 0.45
HANDLE_MAX_BARS = 12
HANDLE_MAX_PULLBACK = 0.12
SYMMETRY_MAX_DIFF = 0.10

# Smoothing
SMA_LENGTH = 5

# Verbose / UI
VERBOSE = False
USE_TQDM = _HAS_TQDM

# ------------------ Alpaca Setup ------------------
load_dotenv("alpkey.env")
ALPACA_API_KEY = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise RuntimeError("Alpaca keys missing in alpkey.env (APCA_API_KEY_ID / APCA_API_SECRET_KEY).")

api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, BASE_URL, api_version="v2")

_last_request_time = 0.0

# ------------------ Helper Functions ------------------
def rate_limited_get_bars(symbol, timeframe, start, end, limit=1000, adjustment="all", feed="iex"):
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < RATE_LIMIT_DELAY:
        time.sleep(RATE_LIMIT_DELAY - elapsed)

    bars = api.get_bars(symbol, timeframe, start=start, end=end, limit=limit, adjustment=adjustment, feed=feed)
    _last_request_time = time.time()

    if bars is None or bars.df.empty:
        return None

    df = bars.df

    if isinstance(df.index, pd.MultiIndex):
        try:
            if symbol in df.index.levels[0]:
                df = df.xs(symbol, level=0)
            elif "symbol" in df.columns:
                df = df[df["symbol"] == symbol].drop(columns=["symbol"], errors="ignore")
            else:
                return None
        except Exception:
            return None

    if not isinstance(df.index, pd.DatetimeIndex):
        if "timestamp" in df.columns:
            df = df.reset_index().set_index("timestamp")
        elif "t" in df.columns:
            df = df.reset_index().set_index("t")
        else:
            try:
                df.index = pd.to_datetime(df.index)
            except Exception:
                return None

    df.columns = [c.lower() for c in df.columns]
    keep_cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
    df = df.loc[:, keep_cols].sort_index()
    return df

# ------------------ Detection ------------------
def detect_cup_handle(df):
    n = len(df)
    if n < (CUP_MIN_BARS + 3):
        return False

    df = df.copy()
    df["sma"] = ta.sma(df["close"], length=SMA_LENGTH)
    sma = df["sma"].dropna()
    if len(sma) < (CUP_MIN_BARS // 2):
        return False

    handle_len = min(HANDLE_MAX_BARS, max(1, int(len(df) * 0.15)))
    handle_start_idx = len(df) - handle_len
    if handle_start_idx <= 1:
        return False

    cup_df = df.iloc[:handle_start_idx].copy()
    handle_df = df.iloc[handle_start_idx:].copy()

    if len(cup_df) < CUP_MIN_BARS or len(cup_df) > CUP_MAX_BARS * 10:
        if len(cup_df) < CUP_MIN_BARS:
            return False

    cup_df["sma"] = ta.sma(cup_df["close"], length=SMA_LENGTH)
    cup_sma = cup_df["sma"].dropna()
    if cup_sma.empty or len(cup_sma) < 3:
        return False

    left_lip = cup_sma.iloc[0]
    right_lip = cup_sma.iloc[-1]
    cup_low = cup_sma.min()

    denom = max(left_lip, right_lip)
    if denom <= 0 or math.isnan(denom):
        return False
    depth = (denom - cup_low) / denom
    if depth < CUP_MIN_DEPTH or depth > CUP_MAX_DEPTH:
        return False

    if abs(left_lip - right_lip) / denom > SYMMETRY_MAX_DIFF:
        return False

    handle_low = handle_df["close"].min()
    handle_pullback = (right_lip - handle_low) / right_lip
    if handle_pullback > HANDLE_MAX_PULLBACK:
        return False

    current_close = df["close"].iloc[-1]
    if current_close < right_lip * (1 - HANDLE_MAX_PULLBACK):
        return False

    bottom_pos = cup_sma.idxmin()
    bottom_rel_idx = cup_sma.index.get_loc(bottom_pos)
    if bottom_rel_idx < len(cup_sma) * 0.10 or bottom_rel_idx > len(cup_sma) * 0.90:
        return False

    return True

# ------------------ Visualization ------------------
def plot_cup_handle(df, ticker):
    df = df.copy()
    df["sma"] = ta.sma(df["close"], length=SMA_LENGTH)
    handle_len = min(HANDLE_MAX_BARS, max(1, int(len(df) * 0.15)))
    handle_start_idx = len(df) - handle_len

    cup_df = df.iloc[:handle_start_idx].copy()
    handle_df = df.iloc[handle_start_idx:].copy()

    left_lip = cup_df["sma"].iloc[0]
    right_lip = cup_df["sma"].iloc[-1]
    cup_low = cup_df["sma"].min()
    current_close = df["close"].iloc[-1]

    plt.figure(figsize=(10, 6))
    plt.plot(df.index, df["close"], label="Close", color="blue")
    plt.plot(cup_df.index, cup_df["sma"], label="Cup SMA", color="green", linewidth=2)
    plt.plot(handle_df.index, handle_df["close"], label="Handle", color="orange", linewidth=2)
    plt.scatter(cup_df.index[0], left_lip, color="red", s=50, label="Left Lip")
    plt.scatter(cup_df.index[-1], right_lip, color="red", s=50, label="Right Lip")
    bottom_idx = cup_df["sma"].idxmin()
    plt.scatter(bottom_idx, cup_low, color="purple", s=50, label="Cup Bottom")
    plt.scatter(df.index[-1], current_close, color="black", s=50, label="Current Close")

    plt.title(f"Cup & Handle Pattern: {ticker}")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # Save to file instead of showing
    out_dir = "plots"
    os.makedirs(out_dir, exist_ok=True)
    plt.savefig(os.path.join(out_dir, f"{ticker}_cup_handle.png"))
    plt.close()  # close the figure to free memory


# ------------------ Main ------------------
def main():
    try:
        df_t = pd.read_csv(TICKERS_FILE, header=0)
        if df_t.shape[1] == 0:
            print("No columns in tickers file.")
            return
        tickers = df_t.iloc[:, 0].dropna().astype(str).str.strip().unique().tolist()
    except FileNotFoundError:
        print(f"Tickers file '{TICKERS_FILE}' not found.")
        return
    except Exception as e:
        print(f"Failed to load tickers: {e}")
        return

    if VERBOSE:
        print(f"Loaded {len(tickers)} tickers from {TICKERS_FILE} (first 10): {tickers[:10]}")

    end = datetime.now()
    start = end - timedelta(days=120)

    results = []
    iterator = tqdm(tickers, desc="Scanning", unit="ticker") if (USE_TQDM and USE_TQDM == True) else tickers

    for ticker in iterator:
        if VERBOSE:
            print(f"→ {ticker}")

        try:
            df = rate_limited_get_bars(ticker, TIMEFRAME, start=start.date().isoformat(), end=end.date().isoformat())
            if df is None:
                if VERBOSE:
                    print(f"  no data for {ticker}")
                continue

            if detect_cup_handle(df):
                results.append(ticker)
                if VERBOSE:
                    print(f"  MATCH: {ticker}")
                plot_cup_handle(df, ticker)

        except Exception as e:
            if VERBOSE:
                print(f"  error for {ticker}: {e}")
            continue

    print("\n📊 Scan complete.")
    if results:
        print(f"✅ Cup & Handle candidates ({len(results)}):")
        for t in results:
            print(f" - {t}")
    else:
        print("No Cup & Handle patterns detected.")

if __name__ == "__main__":
    main()
