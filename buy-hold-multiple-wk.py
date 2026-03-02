#!/usr/bin/env python3
"""
fetch_bh_metrics.py
- Input: tickers.csv (Ticker,IPO Date) or list in code
- Output: results.csv with per-ticker buy-and-hold return and max drawdown
- Caching: stores per-ticker JSON in ./cache/, invalidates after 30 days
- Rate limiting: aims to keep <= 180 reqs/min (safer than 200) and backoffs on 429
"""

import os
import time
import json
import math
import pickle
import traceback
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
import numpy as np
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv
import requests  # used for detecting 429 responses if needed

# --- CONFIG ---
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
CACHE_MAX_AGE_DAYS = 30
ALPACA_ENV_FILE = "alpkey.env"
OUTPUT_CSV = "results.csv"
TICKERS_CSV = "tickers.csv"  # expected columns: Ticker, IPO Date (optional)
# Rate limiting: target interval between requests (seconds). 60/180 = 0.333s -> we use 0.35s
REQUEST_INTERVAL = 0.35
MAX_RETRIES = 5
RETRY_BACKOFF_FACTOR = 2.0

# --- helpers ---------------------------------------------------------------
def parse_date(s):
    if pd.isna(s) or s is None or str(s).strip() == "":
        return None
    for fmt in ("%Y-%m-%d", "%Y", "%b %d, %Y", "%B %d, %Y", "%b %Y", "%B %Y", "%d %b %Y", "%d %B %Y", "%b %d %Y"):
        try:
            return datetime.strptime(str(s).strip(), fmt).date()
        except Exception:
            pass
    # try pandas fallback
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return None

def cache_path(ticker):
    return CACHE_DIR / f"{ticker.upper()}.pkl"

def load_cache(ticker):
    p = cache_path(ticker)
    if not p.exists():
        return None
    try:
        mtime = datetime.fromtimestamp(p.stat().st_mtime)
        if datetime.now() - mtime > timedelta(days=CACHE_MAX_AGE_DAYS):
            # expired
            return None
        with open(p, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None

def save_cache(ticker, obj):
    p = cache_path(ticker)
    with open(p, "wb") as f:
        pickle.dump(obj, f)

def compute_buyhold_metrics(df, start_dt, end_dt):
    # df: pandas DataFrame with columns ['Open','High','Low','Close','Volume'] and datetime index
    # consider only rows between start_dt and end_dt inclusive
    df = df.loc[(df.index.date >= start_dt) & (df.index.date <= end_dt)].copy()
    if df.empty:
        return None
    # buy at first Close, sell at last Close
    entry_price = float(df['Close'].iloc[0])
    exit_price = float(df['Close'].iloc[-1])
    ret_pct = (exit_price / entry_price - 1) * 100.0

    # equity series: assume invest 1 unit of capital converted to price basis
    equity = df['Close'] / entry_price  # starting 1.0, ending = 1 + ret
    # drawdown
    cum_max = equity.cummax()
    drawdown = (equity / cum_max - 1.0) * 100.0  # negative values
    max_dd = float(drawdown.min())
    return {"return_pct": ret_pct, "max_drawdown_pct": max_dd, "n_bars": len(df)}

# Simple rate-limited fetch wrapper with retries/backoff
_last_request_time = 0.0
def rate_limited_get_bars(api, symbol, timeframe, start, end, limit=None):
    global _last_request_time
    for attempt in range(1, MAX_RETRIES + 1):
        # enforce minimal interval
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - elapsed)
        try:
            bars = api.get_bars(symbol, timeframe, start=start, end=end, limit=limit, feed="iex")
            _last_request_time = time.time()
            return bars
        except tradeapi.rest.APIError as e:
            # alpaca-trade-api raises APIError on 429 typically; fallback to inspect e.status if available
            text = str(e)
            if "429" in text or "Too Many Requests" in text or hasattr(e, 'status') and getattr(e, 'status') == 429:
                wait = (RETRY_BACKOFF_FACTOR ** (attempt - 1)) * REQUEST_INTERVAL * 4
                print(f"[{symbol}] 429 rate limit — backing off {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            else:
                # other API error; re-raise after attempts
                print(f"[{symbol}] APIError: {e}")
                raise
        except Exception as e:
            # network or other issues
            wait = (RETRY_BACKOFF_FACTOR ** (attempt - 1)) * REQUEST_INTERVAL * 2
            print(f"[{symbol}] Error fetching bars: {e}. Backoff {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
            last = e
            continue
    # if we exit loop
    raise RuntimeError(f"Failed to fetch bars for {symbol} after {MAX_RETRIES} attempts. Last error: {last}")

# --- main logic -----------------------------------------------------------
def main():
    # load env keys
    load_dotenv(ALPACA_ENV_FILE)
    API_KEY = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY") or os.getenv("APCA-API-KEY-ID") or os.getenv("APCA_API_KEY")
    SECRET_KEY = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA-API-SECRET-KEY") or os.getenv("APCA_API_SECRET")
    BASE_URL = os.getenv("APCA_BASE_URL") or "https://paper-api.alpaca.markets"
    if not API_KEY or not SECRET_KEY:
        raise SystemExit("API keys not found in environment. Put keys in alpkey.env with either APCA_API_KEY_ID / APCA_API_SECRET_KEY or ALPACA_API_KEY / ALPACA_SECRET_KEY.")

    api = tradeapi.REST(key_id=API_KEY, secret_key=SECRET_KEY, base_url=BASE_URL, api_version="v2")

    # load tickers
    if Path(TICKERS_CSV).exists():
        df_t = pd.read_csv(TICKERS_CSV, dtype=str)
        if 'Ticker' not in df_t.columns:
            raise SystemExit(f"{TICKERS_CSV} must have a 'Ticker' column.")
        tickers = []
        for _, row in df_t.iterrows():
            t = str(row['Ticker']).strip()
            ipo = parse_date(row.get('IPO Date', None)) if 'IPO Date' in row.index or 'IPO Date' in df_t.columns else None
            tickers.append((t, ipo))
    else:
        # fallback: paste your list here manually if no CSV
        tickers = [
            # ('APP','2021-04-15'),
            # add tuples (ticker, ipo_date_or_None)
        ]
        if not tickers:
            raise SystemExit("No tickers provided. Create tickers.csv with columns: Ticker, IPO Date")

    results = []
    today = date.today()
    five_years_ago = today - timedelta(days=5*365)

    for ticker, ipo_date in tickers:
        ticker = ticker.strip().upper()
        print(f"\nProcessing {ticker} (IPO: {ipo_date}) ...")
        try:
            # determine desired start_date
            if ipo_date:
                # if IPO earlier than 5y, use only last 5 years
                start_date = max(parse_date(ipo_date), five_years_ago)
            else:
                start_date = five_years_ago  # fallback if no IPO date

            end_date = today - timedelta(days=1)  # "yesterday"

            # check cache
            cached = load_cache(ticker)
            if cached:
                # cached is a dict with 'fetched_at' and 'bars_df' or 'raw'
                print(f" - using cached data (fetched {cached['fetched_at']})")
                bars_df = cached['bars_df']
            else:
                # fetch from Alpaca (daily bars)
                print(f" - fetching bars from Alpaca: {start_date} -> {end_date}")
                # Alpaca get_bars sometimes requires ISO format
                start_iso = start_date.isoformat()
                end_iso = (end_date + timedelta(days=1)).isoformat()  # inclusive handling
                bars_resp = rate_limited_get_bars(api, ticker, tradeapi.TimeFrame.Day, start=start_iso, end=end_iso)
                # convert to DataFrame and filter symbol
                bars_df = bars_resp.df if hasattr(bars_resp, "df") else pd.DataFrame(bars_resp)
                # some responses include many symbols; filter
                if "symbol" in bars_df.columns:
                    bars_df = bars_df[bars_df.symbol == ticker].copy()
                if bars_df.empty:
                    print(f" - no bars returned for {ticker}. Recording note and skipping.")
                    results.append({"Ticker": ticker, "IPO Date": ipo_date or "", "Start Date": start_date.isoformat(),
                                    "End Date": end_date.isoformat(), "Return [%]": "", "Max Drawdown [%]": "", "Notes": "no data"})
                    continue
                # format to OHLCV expected
                bars_df = bars_df.rename(columns={c: c.capitalize() for c in bars_df.columns})
                # keep Open High Low Close Volume
                expected_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                for c in expected_cols:
                    if c not in bars_df.columns:
                        bars_df[c] = np.nan
                bars_df.index = pd.to_datetime(bars_df.index)
                # cache
                save_cache(ticker, {"fetched_at": datetime.now().isoformat(), "bars_df": bars_df})
                print(f" - cached {ticker} data")

            # compute metrics
            metrics = compute_buyhold_metrics(bars_df, start_date, end_date)
            if metrics is None:
                notes = "no overlap in requested date range"
                results.append({"Ticker": ticker, "IPO Date": ipo_date or "", "Start Date": start_date.isoformat(),
                                "End Date": end_date.isoformat(), "Return [%]": "", "Max Drawdown [%]": "", "Notes": notes})
            else:
                results.append({"Ticker": ticker,
                                "IPO Date": ipo_date.isoformat() if ipo_date else "",
                                "Start Date": start_date.isoformat(),
                                "End Date": end_date.isoformat(),
                                "Return [%]": round(metrics["return_pct"], 6),
                                "Max Drawdown [%]": round(metrics["max_drawdown_pct"], 6),
                                "Notes": ""})
        except Exception as e:
            traceback.print_exc()
            results.append({"Ticker": ticker, "IPO Date": ipo_date or "", "Start Date": "",
                            "End Date": "", "Return [%]": "", "Max Drawdown [%]": "", "Notes": f"error: {e}"})
        # end for ticker
    # write results
    df_out = pd.DataFrame(results)
    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"\nDone. Wrote {OUTPUT_CSV} with {len(results)} rows.")

if __name__ == "__main__":
    main()
