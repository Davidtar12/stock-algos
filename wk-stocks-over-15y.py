import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import os
import requests
import json
from dotenv import load_dotenv
load_dotenv()


# --- Credentials ---
LOGIN = ADMIRALS_ACCOUNT
PASSWORD = os.getenv('ADMIRAL_PASSWORD')
SERVER = "AdmiralsSC-Demo"
MT5_PATH = r"C:\Program Files\Admirals SC MT5 Terminal\terminal64.exe"

# --- Finnhub API Key ---
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')

# --- Cache Files ---
CACHE_FILE = "us_stock_symbols_cache.txt"
MARKET_CAP_CACHE_FILE = "market_cap_cache.json"
CACHE_EXPIRY_DAYS = 7  # Market cap cache expires after 7 days

# --- Progress Bar ---
def print_progress(iteration, total, prefix='', length=20):
    percent = int(100 * iteration / total)
    filled = int(length * iteration / total)
    bar = '#' * filled + '-' * (length - filled)
    print(f'\r{prefix} [{bar}] {percent}% ({iteration}/{total})', end='', flush=True)

# --- Initialize MT5 ---
def initialize_mt5():
    if not mt5.initialize(path=MT5_PATH, login=LOGIN, password=PASSWORD, server=SERVER):
        print(f"❌ initialize() failed, error code = {mt5.last_error()}")
        quit()

# --- Fetch US Stock Symbols ---
def get_us_stock_symbols():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            symbols = [line.strip() for line in f]
            print(f"✅ Loaded {len(symbols)} symbols from cache.")
            return symbols

    all_symbols = mt5.symbols_get()
    if all_symbols is None:
        print("❌ Failed to retrieve symbols from MT5.")
        return []

    stock_paths = ["T-Stock CFDs\\US (NASDAQ)", "T-Stock CFDs\\US (NYSE)"]
    symbols = list(set(s.name for s in all_symbols if any(s.path.startswith(p) for p in stock_paths)))

    with open(CACHE_FILE, "w") as f:
        for symbol in symbols:
            f.write(symbol + "\n")

    print(f"💾 Cached {len(symbols)} symbols.")
    return symbols

# --- Format Ticker for Finnhub ---
def format_ticker_for_finnhub(admiral_ticker):
    t = admiral_ticker.lstrip('#')
    if t.endswith('.US-T'): return t[:-5]
    if t.endswith('-T'): return t[:-2]
    return t

# --- Load Market Cap Cache ---
def load_market_cap_cache():
    if not os.path.exists(MARKET_CAP_CACHE_FILE):
        return {}

    try:
        with open(MARKET_CAP_CACHE_FILE, "r") as f:
            cache = json.load(f)

        cache_date = datetime.fromisoformat(cache.get("last_updated", "1970-01-01"))
        if (datetime.now() - cache_date).days > CACHE_EXPIRY_DAYS:
            print("📅 Market cap cache expired.")
            return {}

        print(f"✅ Loaded market cap cache with {len(cache.get('data', {}))} entries.")
        return cache.get("data", {})
    except:
        return {}

# --- Save Market Cap Cache ---
def save_market_cap_cache(cache_data):
    cache = {
        "last_updated": datetime.now().isoformat(),
        "data": cache_data
    }
    with open(MARKET_CAP_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

# --- Filter by Market Cap ---
def prefilter_by_market_cap(symbols, min_cap_millions):
    market_cap_cache = load_market_cap_cache()
    filtered = []
    cache_updated = False
    total = len(symbols)

    for i, symbol in enumerate(symbols, 1):
        print_progress(i, total, prefix="🔎 Market Cap Filter")
        finnhub_symbol = format_ticker_for_finnhub(symbol)
        if not finnhub_symbol:
            continue

        if finnhub_symbol in market_cap_cache:
            cap = market_cap_cache[finnhub_symbol]
            if cap and cap > min_cap_millions:
                filtered.append(symbol)
            continue

        try:
            url = f"https://finnhub.io/api/v1/stock/profile2?symbol={finnhub_symbol}&token={FINNHUB_API_KEY}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            profile = response.json()
            cap = profile.get("marketCapitalization")

            market_cap_cache[finnhub_symbol] = cap
            cache_updated = True

            if cap and cap > min_cap_millions:
                filtered.append(symbol)
        except:
            market_cap_cache[finnhub_symbol] = None
            cache_updated = True
            continue

        time.sleep(1.1)  # Rate limit

    if cache_updated:
        save_market_cap_cache(market_cap_cache)
        print(f"\n💾 Updated market cap cache with {len(market_cap_cache)} entries.")
    print()
    return filtered

# --- Return from 2000 to 2021 ---
def get_return_2000_to_2021(symbol):
    try:
        start = datetime(2000, 1, 3)
        end = datetime(2021, 12, 31)
        rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_D1, start, end)
        if rates is None or len(rates) < 2:
            return None

        df = pd.DataFrame(rates)
        if df.empty or df.iloc[0]['open'] == 0:
            return None

        open_price = float(df.iloc[0]['open'])
        close_price = float(df.iloc[-1]['close'])
        return_pct = ((close_price / open_price) - 1) * 100
        return return_pct
    except:
        return None

# --- Main Screener ---
def run_screener():
    initialize_mt5()
    us_symbols = get_us_stock_symbols()
    if not us_symbols:
        mt5.shutdown()
        return

    us_symbols = prefilter_by_market_cap(us_symbols, 100000)  # Market cap > $100B
    if not us_symbols:
        mt5.shutdown()
        return

    qualified = []
    total = len(us_symbols)

    for i, symbol in enumerate(us_symbols, 1):
        print_progress(i, total, prefix="📈 Return Analysis")

        if not mt5.symbol_select(symbol, True):
            continue

        ret = get_return_2000_to_2021(symbol)
        if ret is not None and ret > 400:
            qualified.append({"Symbol": symbol, "Return 2000-2021 (%)": ret})

        time.sleep(0.1)

    mt5.shutdown()

    if qualified:
        qualified_sorted = sorted(qualified, key=lambda x: x["Return 2000-2021 (%)"], reverse=True)
        print(f"\n📈 Found {len(qualified_sorted)} stocks with >400% return from 2000 to 2021:\n")
        for stock in qualified_sorted:
            print(f"{stock['Symbol']}: {stock['Return 2000-2021 (%)']:.2f}%")
    else:
        print("\n❌ No stocks met the return criteria.")

# --- Entry Point ---
if __name__ == "__main__":
    run_screener()
