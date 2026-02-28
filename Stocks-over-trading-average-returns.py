import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import requests
from dotenv import load_dotenv
import os
load_dotenv()


# --- Credentials ---
LOGIN = ADMIRALS_ACCOUNT
PASSWORD = os.getenv('ADMIRAL_PASSWORD')
SERVER = "AdmiralsSC-Demo"
MT5_PATH = r"C:\Program Files\Admirals SC MT5 Terminal\terminal64.exe"

# --- Finnhub API Key ---
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')

# --- Return Thresholds (% over years) ---
RETURNS_CRITERIA = {
    15: 225,
    10: 150,
    5:  50,
    3:  45
}

# --- Progress Bar Function ---
def print_progress(iteration, total, prefix='', length=20):
    percent = int(100 * iteration / total)
    filled = int(length * iteration / total)
    bar = '#' * filled + '-' * (length - filled)
    print(f'\r{prefix} [{bar}] {percent}% ({iteration}/{total})', end='', flush=True)

# --- MT5 Initialization ---
def initialize_mt5():
    if not mt5.initialize(path=MT5_PATH, login=LOGIN, password=PASSWORD, server=SERVER):
        print(f"❌ initialize() failed, error code = {mt5.last_error()}")
        quit()

# --- Fetch US Stocks from Admirals ---
def get_us_stock_symbols():
    all_symbols = mt5.symbols_get()
    if all_symbols is None:
        return []
    stock_paths = ["T-Stock CFDs\\US (NASDAQ)", "T-Stock CFDs\\US (NYSE)"]
    return list(set(s.name for s in all_symbols if any(s.path.startswith(p) for p in stock_paths)))

# --- Admirals Ticker -> Finnhub Ticker ---
def format_ticker_for_finnhub(admiral_ticker):
    t = admiral_ticker.lstrip('#')
    if t.endswith('.US-T'): return t[:-5]
    if t.endswith('-T'): return t[:-2]
    return t

# --- Market Cap Filter via Finnhub ---
def prefilter_by_market_cap(symbols, min_cap_millions):
    filtered = []
    total = len(symbols)
    for i, symbol in enumerate(symbols, 1):
        print_progress(i, total, prefix="🔎 Market Cap Filter")
        finnhub_symbol = format_ticker_for_finnhub(symbol)
        if not finnhub_symbol:
            continue
        try:
            url = f"https://finnhub.io/api/v1/stock/profile2?symbol={finnhub_symbol}&token={FINNHUB_API_KEY}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            profile = response.json()
            cap = profile.get("marketCapitalization")
            if cap and cap > min_cap_millions:
                filtered.append(symbol)
        except:
            continue
        time.sleep(1.1)  # Finnhub rate limit
    print()  # Move to next line
    return filtered

# --- % Return over Period ---
def get_return_for_period(symbol, years):
    end = datetime.now()
    start = end - relativedelta(years=years)
    rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_D1, start, end)
    if not rates or len(rates) < 2:
        return None
    df = pd.DataFrame(rates)
    if df.empty: return None
    open_price = df.iloc[0]['open']
    close_price = df.iloc[-1]['close']
    if open_price == 0: return None
    return ((close_price / open_price) - 1) * 100

# --- Main Screener Logic ---
def run_screener():
    initialize_mt5()
    us_symbols = get_us_stock_symbols()
    if not us_symbols:
        mt5.shutdown()
        return

    us_symbols = prefilter_by_market_cap(us_symbols, 10000)
    if not us_symbols:
        mt5.shutdown()
        return

    qualified = []
    total = len(us_symbols)
    for i, symbol in enumerate(us_symbols, 1):
        print_progress(i, total, prefix="📈 Return Analysis")
        if not mt5.symbol_select(symbol, True):
            continue
        try:
            r15 = get_return_for_period(symbol, 15)
            r10 = get_return_for_period(symbol, 10)
            r5  = get_return_for_period(symbol, 5)
            r3  = get_return_for_period(symbol, 3)
            if None in (r15, r10, r5, r3):
                continue
            if r15 > RETURNS_CRITERIA[15] and r10 > RETURNS_CRITERIA[10] and r5 > RETURNS_CRITERIA[5] and r3 > RETURNS_CRITERIA[3]:
                qualified.append({
                    "Symbol": symbol,
                    "Return 5Y (%)": f"{r5:.2f}",
                    "Return 10Y (%)": f"{r10:.2f}",
                    "Return 15Y (%)": f"{r15:.2f}",
                })
        except:
            continue
        time.sleep(0.05)  # MT5 pacing
    print()

    mt5.shutdown()

    print("\n" + "="*80)
    print("🚀 Screener Results: Qualified Stocks")
    print("="*80)
    if qualified:
        df = pd.DataFrame(qualified)
        print(df.to_string(index=False))
    else:
        print("No stocks met all the specified criteria.")

# --- Run ---
if __name__ == "__main__":
    run_screener()
