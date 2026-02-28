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

# --- Return Thresholds (% over years) ---
RETURNS_CRITERIA = {
    15: 225,
    10: 150,
    5:  50,
    3:  45
}

# --- Cache Files ---
CACHE_FILE = "us_stock_symbols_cache.txt"
MARKET_CAP_CACHE_FILE = "market_cap_cache.json"
CACHE_EXPIRY_DAYS = 7  # Market cap cache expires after 7 days


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
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            symbols = [line.strip() for line in f]
            print(f"✅ Loaded {len(symbols)} symbols from cache.")
            return symbols

    else:
        all_symbols = mt5.symbols_get()
        if all_symbols is None:
            print("❌ Failed to retrieve symbols from MT5.")
            return []
        stock_paths = ["T-Stock CFDs\\US (NASDAQ)", "T-Stock CFDs\\US (NYSE)"]
        symbols = list(set(s.name for s in all_symbols if any(s.path.startswith(p) for p in stock_paths)))
        with open(CACHE_FILE, "w") as f:
            for symbol in symbols:
                f.write(symbol + "\n")
        print(f"💾 Cached {len(symbols)} symbols for future use.")
        return symbols

# --- Admirals Ticker -> Finnhub Ticker ---
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
        
        # Check if cache is expired
        cache_date = datetime.fromisoformat(cache.get("last_updated", "1970-01-01"))
        if (datetime.now() - cache_date).days > CACHE_EXPIRY_DAYS:
            print("📅 Market cap cache expired, will refresh.")
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

# --- Market Cap Filter via Finnhub ---
def prefilter_by_market_cap(symbols, min_cap_millions):
    # Load existing cache
    market_cap_cache = load_market_cap_cache()
    
    filtered = []
    cache_updated = False
    total = len(symbols)
    
    for i, symbol in enumerate(symbols, 1):
        print_progress(i, total, prefix="🔎 Market Cap Filter")
        finnhub_symbol = format_ticker_for_finnhub(symbol)
        if not finnhub_symbol:
            continue
        
        # Check cache first
        if finnhub_symbol in market_cap_cache:
            cap = market_cap_cache[finnhub_symbol]
            if cap and cap > min_cap_millions:
                filtered.append(symbol)
            continue
        
        # If not in cache, fetch from API
        try:
            url = f"https://finnhub.io/api/v1/stock/profile2?symbol={finnhub_symbol}&token={FINNHUB_API_KEY}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            profile = response.json()
            cap = profile.get("marketCapitalization")
            
            # Cache the result (even if None)
            market_cap_cache[finnhub_symbol] = cap
            cache_updated = True
            
            if cap and cap > min_cap_millions:
                filtered.append(symbol)
        except:
            # Cache failed requests as None to avoid retrying
            market_cap_cache[finnhub_symbol] = None
            cache_updated = True
            continue
        
        time.sleep(1.1)  # Finnhub rate limit
    
    # Save updated cache
    if cache_updated:
        save_market_cap_cache(market_cap_cache)
        print(f"\n💾 Updated market cap cache with {len(market_cap_cache)} entries.")
    
    print()  # Move to next line
    return filtered

# --- % Return over Period ---
def get_return_for_period(symbol, years):
    try:
        end = datetime.now()
        start = end - relativedelta(years=years)
        
        print(f"\n🔍 DEBUG: Processing {symbol} for {years} years")
        print(f"🔍 DEBUG: Date range: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
        
        rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_D1, start, end)
        if rates is None or len(rates) < 2:
            if rates is None:
                print(f"🔍 DEBUG: No rates returned for {symbol}")
            else:
                print(f"🔍 DEBUG: Insufficient data for {symbol} - only {len(rates)} records")
            return None
            
        print(f"🔍 DEBUG: Got {len(rates)} rates for {symbol}")
        
        df = pd.DataFrame(rates)
        if df.empty:
            print(f"🔍 DEBUG: Empty DataFrame for {symbol}")
            return None
        
        print(f"🔍 DEBUG: DataFrame shape: {df.shape}")
        print(f"🔍 DEBUG: DataFrame columns: {df.columns.tolist()}")
        print(f"🔍 DEBUG: First row open type: {type(df.iloc[0]['open'])}")
        print(f"🔍 DEBUG: First row open value: {df.iloc[0]['open']}")
        print(f"🔍 DEBUG: Last row close type: {type(df.iloc[-1]['close'])}")
        print(f"🔍 DEBUG: Last row close value: {df.iloc[-1]['close']}")
        
        # Convert to scalar values to avoid pandas array comparison issues
        open_price = float(df.iloc[0]['open'])
        close_price = float(df.iloc[-1]['close'])
        
        print(f"🔍 DEBUG: Converted open_price: {open_price} (type: {type(open_price)})")
        print(f"🔍 DEBUG: Converted close_price: {close_price} (type: {type(close_price)})")
        
        if open_price == 0:
            print(f"🔍 DEBUG: Zero open price for {symbol}")
            return None
        
        return_pct = ((close_price / open_price) - 1) * 100
        print(f"🔍 DEBUG: Calculated return for {symbol}: {return_pct:.2f}%")
        
        return return_pct
        
    except Exception as e:
        print(f"🔍 DEBUG: Exception in get_return_for_period for {symbol}: {e}")
        print(f"🔍 DEBUG: Exception type: {type(e)}")
        import traceback
        traceback.print_exc()
        return None

# --- Main Screener Logic ---
def run_screener():
    initialize_mt5()
    us_symbols = get_us_stock_symbols()
    if not us_symbols:
        mt5.shutdown()
        return

    us_symbols = prefilter_by_market_cap(us_symbols, 100000)
    if not us_symbols:
        mt5.shutdown()
        return

    qualified = []
    total = len(us_symbols)
    for i, symbol in enumerate(us_symbols, 1):
        print_progress(i, total, prefix="📈 Return Analysis")
        
        print(f"\n🔍 DEBUG: Starting analysis for symbol {i}/{total}: {symbol}")
        
        if not mt5.symbol_select(symbol, True):
            print(f"❌ Could not select symbol: {symbol}")
            continue
            
        print(f"🔍 DEBUG: Successfully selected symbol: {symbol}")
        
        try:
            print(f"🔍 DEBUG: Getting 10Y return for {symbol}")
            r10 = get_return_for_period(symbol, 10)
            print(f"🔍 DEBUG: 10Y return result: {r10}")
            
            print(f"🔍 DEBUG: Getting 5Y return for {symbol}")
            r5  = get_return_for_period(symbol, 5)
            print(f"🔍 DEBUG: 5Y return result: {r5}")
            
            print(f"🔍 DEBUG: Getting 3Y return for {symbol}")
            r3  = get_return_for_period(symbol, 3)
            print(f"🔍 DEBUG: 3Y return result: {r3}")
            
            # Fixed: Check each return individually instead of using 'in' with tuple
            if r10 is None or r5 is None or r3 is None:
                print(f"🔍 DEBUG: Skipping {symbol} - one or more returns is None")
                continue
                
            print(f"🔍 DEBUG: Checking criteria for {symbol}: r10={r10}, r5={r5}, r3={r3}")
            print(f"🔍 DEBUG: Criteria: r10 > {RETURNS_CRITERIA[10]}, r5 > {RETURNS_CRITERIA[5]}, r3 > {RETURNS_CRITERIA[3]}")
                
            if r10 > RETURNS_CRITERIA[10] and r5 > RETURNS_CRITERIA[5] and r3 > RETURNS_CRITERIA[3]:
                print(f"✅ {symbol} QUALIFIED!")
                qualified.append({
                    "Symbol": symbol,
                    "Return 5Y (%)": f"{r5:.2f}",
                    "Return 10Y (%)": f"{r10:.2f}",
                })
            else:
                print(f"❌ {symbol} did not meet criteria")
                
        except Exception as e:
            print(f"⚠️ Error processing {symbol}: {e}")
            print(f"🔍 DEBUG: Exception type: {type(e)}")
            import traceback
            traceback.print_exc()
            continue
            
        time.sleep(0.1)  # MT5 pacing

    print()  # Clean newline after progress
    
    # Display results
    if qualified:
        print(f"\n✅ Found {len(qualified)} qualified stocks:")
        for stock in qualified:
            print(f"  {stock['Symbol']}: 5Y={stock['Return 5Y (%)']}%, 10Y={stock['Return 10Y (%)']}%")
    else:
        print("\n❌ No stocks met the criteria.")
    
    mt5.shutdown()

if __name__ == "__main__":
    run_screener()
