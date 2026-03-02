#!/usr/bin/env python3
"""
fetch_bh_metrics.py
- Input: tickers.csv (Ticker,IPO Date)
- Output: results.csv with per-ticker buy-and-hold return and max drawdown
- Caching: stores per-ticker data in ./cache/, invalidates after 30 days
- Rate limiting: aims to keep <= 180 reqs/min and backoffs on 429
"""

import os
import time
import pickle
import traceback
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
import numpy as np
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv

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

# --- HELPERS ---
def parse_date(s):
    """Flexibly parse date strings into date objects."""
    if pd.isna(s) or s is None or str(s).strip() == "":
        return None
    for fmt in ("%Y-%m-%d", "%Y", "%b %d, %Y", "%B %d, %Y", "%b %Y", "%B %Y", "%d %b %Y", "%d %B %Y", "%b %d %Y"):
        try:
            return datetime.strptime(str(s).strip(), fmt).date()
        except ValueError:
            pass
    # try pandas fallback
    try:
        return pd.to_datetime(s).date()
    except (ValueError, TypeError):
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
            return None  # Cache expired
        with open(p, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None

def save_cache(ticker, obj):
    p = cache_path(ticker)
    with open(p, "wb") as f:
        pickle.dump(obj, f)

def compute_metrics(df, start_dt, end_dt, analysis_year=2025):
    """Calculate return, max drawdown, and volatility for a given period."""
    df_period = df.loc[(df.index.date >= start_dt) & (df.index.date <= end_dt)].copy()
    if df_period.empty or len(df_period) < 2:
        return None

    entry_price = float(df_period['Close'].iloc[0])
    exit_price = float(df_period['Close'].iloc[-1])

    if entry_price == 0: return None

    ret_pct = (exit_price / entry_price - 1) * 100.0

    # Calculate volatility (annualized standard deviation of daily returns)
    daily_returns = df_period['Close'].pct_change()
    volatility_pct = daily_returns.std() * np.sqrt(252) * 100.0

    # Calculate overall drawdown
    equity = df_period['Close'] / entry_price
    cum_max = equity.cummax()
    drawdown = (equity / cum_max - 1.0) * 100.0
    max_dd = float(drawdown.min())

    # Calculate duration of the maximum drawdown
    if max_dd < 0:
        trough_date = drawdown.idxmin()
        peak_date = equity.loc[:trough_date].idxmax()

        equity_after_trough = equity.loc[trough_date:]
        peak_value = equity.loc[peak_date]

        try:
            # Find the first date where equity recovers to the peak value
            recovery_date = equity_after_trough[equity_after_trough >= peak_value].index[0]
            max_dd_days = (recovery_date - peak_date).days
        except IndexError:
            # Equity never recovered, so drawdown lasts to the end of the period
            max_dd_days = (df_period.index[-1] - peak_date).days
    else:
        max_dd_days = 0

    # --- Year-Specific Drawdown Calculation ---
    df_year = df.loc[df.index.year == analysis_year].copy()
    max_dd_year_pct = 0
    max_dd_year_days = 0
    if not df_year.empty and len(df_year) > 1:
        entry_price_year = float(df_year['Close'].iloc[0])
        if entry_price_year > 0:
            equity_year = df_year['Close'] / entry_price_year
            cum_max_year = equity_year.cummax()
            drawdown_year = (equity_year / cum_max_year - 1.0) * 100.0
            max_dd_year_pct = float(drawdown_year.min())

            if max_dd_year_pct < 0:
                trough_date_year = drawdown_year.idxmin()
                peak_date_year = equity_year.loc[:trough_date_year].idxmax()
                equity_after_trough_year = equity_year.loc[trough_date_year:]
                peak_value_year = equity_year.loc[peak_date_year]
                try:
                    recovery_date_year = equity_after_trough_year[equity_after_trough_year >= peak_value_year].index[0]
                    max_dd_year_days = (recovery_date_year - peak_date_year).days
                except IndexError:
                    max_dd_year_days = (df_year.index[-1] - peak_date_year).days
            else:
                max_dd_year_pct = 0 # Ensure it's 0 if no drawdown

    return {"return_pct": ret_pct, "max_drawdown_pct": max_dd,
            "max_drawdown_days": max_dd_days, "volatility_pct": volatility_pct, "n_bars": len(df_period),
            "entry_price": entry_price, "exit_price": exit_price,
            f"max_dd_{analysis_year}_pct": max_dd_year_pct, f"max_dd_{analysis_year}_days": max_dd_year_days
            }

_last_request_time = 0.0
def rate_limited_get_bars(api, symbol, timeframe, start, end, limit=None, adjustment='all', feed='iex'):
    """A wrapper for api.get_bars that handles rate limiting and retries."""
    global _last_request_time
    for attempt in range(1, MAX_RETRIES + 1):
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - elapsed)
        
        try:
            print(f" - fetching {adjustment} data for {symbol} via {feed.upper()}...", end='\r')
            bars = api.get_bars(symbol, timeframe, start=start, end=end, limit=None, adjustment=adjustment, feed=feed)
            _last_request_time = time.time()
            return bars
        except tradeapi.rest.APIError as e:
            text = str(e)
            if "429" in text or "Too Many Requests" in text:
                wait = (RETRY_BACKOFF_FACTOR ** (attempt - 1)) * REQUEST_INTERVAL * 4
                print(f"[{symbol}] 429 rate limit — backing off {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                print(f"[{symbol}] APIError: {e}")
                raise
        except Exception as e:
            wait = (RETRY_BACKOFF_FACTOR ** (attempt - 1)) * REQUEST_INTERVAL * 2
            print(f"[{symbol}] Error fetching bars: {e}. Backoff {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
            last_error = e
    
    raise RuntimeError(f"Failed to fetch bars for {symbol} after {MAX_RETRIES} attempts. Last error: {last_error if 'last_error' in locals() else 'Unknown'}")

# --- MAIN LOGIC ---
def main():
    load_dotenv(ALPACA_ENV_FILE)
    API_KEY = os.getenv("APCA_API_KEY_ID")
    SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
    BASE_URL = os.getenv("APCA_BASE_URL") or "https://paper-api.alpaca.markets"
    if not API_KEY or not SECRET_KEY:
        raise SystemExit("API keys not found. Ensure alpkey.env contains APCA_API_KEY_ID and APCA_API_SECRET_KEY.")

    api = tradeapi.REST(key_id=API_KEY, secret_key=SECRET_KEY, base_url=BASE_URL, api_version="v2")

    if not Path(TICKERS_CSV).exists():
        raise SystemExit(f"Ticker file not found: '{TICKERS_CSV}'. Please create this file with a 'Ticker' column.")

    df_t = pd.read_csv(TICKERS_CSV, dtype=str)
    if 'Ticker' not in df_t.columns:
        raise SystemExit(f"'{TICKERS_CSV}' must have a 'Ticker' column.")

    tickers = []
    for _, row in df_t.iterrows():
        t = str(row['Ticker']).strip()
        if not t: continue
        ipo = parse_date(row.get('IPO Date')) if 'IPO Date' in df_t.columns else None
        tickers.append((t, ipo))

    if not tickers:
        raise SystemExit(f"No valid tickers found in '{TICKERS_CSV}'.")

    results = []
    today = date.today()
    five_years_ago = today - timedelta(days=5*365)
    one_year_ago = today - timedelta(days=365)
    analysis_year = 2025

    # --- Fetch Benchmark (IAUM) 1-Year Return ---
    iaum_1y_return = None
    print("\nFetching benchmark data for IAUM...")
    try:
        # Use a slightly larger window to ensure we get data
        iaum_bars = rate_limited_get_bars(api, "IAUM", tradeapi.TimeFrame.Day, start=(one_year_ago - timedelta(days=7)).isoformat(), end=today.isoformat(), adjustment='all')
        print(" " * 80, end='\r') # Clear the line
        if not iaum_bars.df.empty:
            iaum_df = iaum_bars.df.loc[iaum_bars.df.index.date >= one_year_ago]
            if len(iaum_df) > 1:
                iaum_1y_return = (iaum_df['close'].iloc[-1] / iaum_df['close'].iloc[0] - 1) * 100.0
                print(f" - IAUM 1-year return: {iaum_1y_return:.2f}%")
            else:
                 print(" - Warning: Not enough recent data for IAUM to calculate 1Y return.")
        else:
            print(" - Warning: No data returned for IAUM.")
    except Exception as e:
        print(f" - Warning: Could not fetch benchmark data for IAUM. Perf vs IAUM will be blank. Error: {e}")

    for ticker, ipo_date in tickers:
        ticker = ticker.strip().upper()
        print(f"\nProcessing {ticker} (IPO: {ipo_date}) ...")
        try:
            start_date = max(ipo_date, five_years_ago) if ipo_date else five_years_ago
            end_date = today - timedelta(days=1)

            # --- Data Fetching & Caching ---
            cached = load_cache(ticker)
            # Validate cache: check for existence and correct keys ('adj_df', 'raw_df')
            if cached and 'adj_df' in cached and 'raw_df' in cached:
                print(f" - using cached data (fetched {cached['fetched_at']})")
                adj_df = cached['adj_df']
                raw_df = cached['raw_df']
            else:
                if cached: # It exists but has the wrong format
                    print(" - cached data has old format. Re-fetching...")
                start_iso = start_date.isoformat()
                end_iso = (end_date + timedelta(days=1)).isoformat()

                # Fetch ADJUSTED data for metrics
                adj_bars_resp = rate_limited_get_bars(api, ticker, tradeapi.TimeFrame.Day, start=start_iso, end=end_iso, adjustment='all')
                adj_df = adj_bars_resp.df
                if "symbol" in adj_df.columns:
                    adj_df = adj_df[adj_df.symbol == ticker].copy()

                # Fetch RAW data for split detection
                raw_bars_resp = rate_limited_get_bars(api, ticker, tradeapi.TimeFrame.Day, start=start_iso, end=end_iso, adjustment='raw')
                raw_df = raw_bars_resp.df
                if "symbol" in raw_df.columns:
                    raw_df = raw_df[raw_df.symbol == ticker].copy()
                
                print(" " * 80, end='\r') # Clear the line

                if adj_df.empty:
                    print(f" - no bars returned for {ticker}. Recording note and skipping.")
                    results.append({"Ticker": ticker, "Notes": "no data"})
                    continue
                
                # Format dataframes
                for df in [adj_df, raw_df]:
                    df.rename(columns={c: c.capitalize() for c in df.columns}, inplace=True)
                    df.index = pd.to_datetime(df.index)
                
                save_cache(ticker, {"fetched_at": datetime.now().isoformat(), "adj_df": adj_df, "raw_df": raw_df})
                print(f" - cached {ticker} data")

            # --- Split Detection ---
            split_adjusted = "No"
            notes = ""
            if not raw_df.empty and not adj_df.empty:
                # Compare the ratio of the first and last close prices
                raw_ratio = raw_df['Close'].iloc[-1] / raw_df['Close'].iloc[0]
                adj_ratio = adj_df['Close'].iloc[-1] / adj_df['Close'].iloc[0]
                if not np.isclose(raw_ratio, adj_ratio, rtol=0.05): # if ratios differ by >5%
                    split_adjusted = "Yes"
                    notes = "Splits/dividends detected"

            # --- Metrics Calculation ---
            metrics = compute_metrics(adj_df, start_date, end_date, analysis_year=analysis_year)

            # --- 1Y Performance vs IAUM ---
            perf_vs_iaum = None
            if iaum_1y_return is not None:
                ticker_1y_df = adj_df.loc[adj_df.index.date >= one_year_ago]
                if len(ticker_1y_df) > 1:
                    ticker_1y_return = (ticker_1y_df['Close'].iloc[-1] / ticker_1y_df['Close'].iloc[0] - 1) * 100.0
                    perf_vs_iaum = ticker_1y_return - iaum_1y_return

            if metrics is None:
                notes += " | no data in requested date range"
                results.append({"Ticker": ticker, "Notes": notes.strip(" |")})
            else:
                results.append({
                    "Ticker": ticker,
                    "IPO Date": ipo_date.isoformat() if ipo_date else "",
                    "Start Date": start_date.isoformat(),
                    "End Date": end_date.isoformat(),
                    "Return [%]": metrics["return_pct"],
                    "Max Drawdown [%]": metrics["max_drawdown_pct"],
                    "Max Drawdown Days": metrics["max_drawdown_days"],
                    f"Max DD % ({analysis_year})": metrics[f"max_dd_{analysis_year}_pct"],
                    f"Max DD Days ({analysis_year})": metrics[f"max_dd_{analysis_year}_days"],
                    "Perf vs IAUM (1Y) [%]": perf_vs_iaum,
                    "Split Adjusted": split_adjusted,
                    "Volatility [%]": metrics["volatility_pct"],
                    "Entry Price": metrics["entry_price"],
                    "Exit Price": metrics["exit_price"],
                    "Bars Count": metrics["n_bars"],
                    "Notes": notes
                })
        except Exception as e:
            traceback.print_exc()
            results.append({"Ticker": ticker, "Notes": f"error: {e}"})

    df_out = pd.DataFrame(results)
    # Format for better readability in CSV
    for col in ["Return [%]", "Max Drawdown [%]", "Volatility [%]", f"Max DD % ({analysis_year})", "Perf vs IAUM (1Y) [%]"]:
        if col in df_out.columns:
            df_out[col] = df_out[col].apply(lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else x)

    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"\nDone. Wrote {OUTPUT_CSV} with {len(results)} rows.")

if __name__ == "__main__":
    main()
