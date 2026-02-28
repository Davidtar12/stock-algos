import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import time
import os
from dotenv import load_dotenv
import sys
import requests
import json
from alpaca_trade_api.rest import REST
from alpaca_trade_api.stream import Stream
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# --- Credentials ---
load_dotenv("alpkey.env")
ALPACA_API_KEY = os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

if not all([ALPACA_API_KEY, ALPACA_SECRET_KEY, FINNHUB_API_KEY]):
    print("Error: API keys not found in alpkey.env.", file=sys.stderr)
    print("Please ensure alpkey.env contains APCA_API_KEY_ID, APCA_API_SECRET_KEY, and FINNHUB_API_KEY.", file=sys.stderr)
    sys.exit(1)

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

# --- API Clients ---
# It's good practice to instantiate clients once and reuse them
try:
    alpaca_api = REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')
    alpaca_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
except Exception as e:
    print(f"❌ Error initializing Alpaca clients: {e}")
    quit()


# --- Progress Bar Function ---
def print_progress(iteration, total, prefix='', length=20):
    """Displays a progress bar in the console."""
    percent = int(100 * iteration / total)
    filled = int(length * iteration / total)
    bar = '#' * filled + '-' * (length - filled)
    print(f'\r{prefix} [{bar}] {percent}% ({iteration}/{total})', end='', flush=True)


# --- Fetch US Stocks from Alpaca ---
def get_us_stock_symbols():
    """
    Fetches all tradable US stock symbols from Alpaca and caches them.
    """
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            symbols = [line.strip() for line in f]
            print(f"✅ Loaded {len(symbols)} symbols from local cache.")
            return symbols
    else:
        print("🔄 Fetching symbols from Alpaca API...")
        try:
            assets = alpaca_api.list_assets(status='active', asset_class='us_equity')
            # Filter for tradable stocks on major exchanges
            symbols = [
                asset.symbol for asset in assets
                if asset.tradable and asset.exchange in ['NASDAQ', 'NYSE', 'ARCA', 'BATS']
            ]
            with open(CACHE_FILE, "w") as f:
                for symbol in symbols:
                    f.write(symbol + "\n")
            print(f"💾 Cached {len(symbols)} symbols for future use.")
            return symbols
        except Exception as e:
            print(f"❌ Failed to retrieve symbols from Alpaca: {e}")
            return []


# --- Load and Save Market Cap Cache ---
def load_market_cap_cache():
    """Loads the market cap cache from a JSON file if it exists and is not expired."""
    if not os.path.exists(MARKET_CAP_CACHE_FILE):
        return {}
    
    try:
        with open(MARKET_CAP_CACHE_FILE, "r") as f:
            cache = json.load(f)
        
        cache_date = datetime.fromisoformat(cache.get("last_updated", "1970-01-01"))
        if (datetime.now() - cache_date).days > CACHE_EXPIRY_DAYS:
            print("\n📅 Market cap cache expired, will refresh.")
            return {}
        
        print(f"✅ Loaded market cap cache with {len(cache.get('data', {}))} entries.")
        return cache.get("data", {})
    except (json.JSONDecodeError, IOError) as e:
        print(f"⚠️ Could not load market cap cache: {e}")
        return {}


def save_market_cap_cache(cache_data):
    """Saves the market cap data to a JSON file."""
    cache = {
        "last_updated": datetime.now().isoformat(),
        "data": cache_data
    }
    with open(MARKET_CAP_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


# --- Market Cap Filter via Finnhub ---
def prefilter_by_market_cap(symbols, min_cap_millions):
    """
    Filters a list of symbols by a minimum market capitalization using Finnhub API.
    Uses caching to avoid redundant API calls.
    """
    market_cap_cache = load_market_cap_cache()
    filtered_symbols = []
    cache_updated = False
    total = len(symbols)
    
    for i, symbol in enumerate(symbols, 1):
        print_progress(i, total, prefix="🔎 Market Cap Filter")
        
        # Alpaca symbols are usually clean, but we handle potential issues
        if not symbol or not isinstance(symbol, str) or '.' in symbol:
            continue
        
        # Check cache first
        if symbol in market_cap_cache:
            cap = market_cap_cache[symbol]
            if cap and cap > min_cap_millions:
                filtered_symbols.append(symbol)
            continue
        
        # If not in cache, fetch from Finnhub API
        try:
            url = f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={FINNHUB_API_KEY}"
            response = requests.get(url, timeout=10)
            response.raise_for_status() # Raises an exception for bad status codes
            profile = response.json()
            # Finnhub returns an empty dict for unknown symbols
            cap = profile.get("marketCapitalization") if profile else None
            
            market_cap_cache[symbol] = cap
            cache_updated = True
            
            if cap and cap > min_cap_millions:
                filtered_symbols.append(symbol)
        except requests.RequestException as e:
            # Cache failed requests as None to avoid retrying them every time
            market_cap_cache[symbol] = None
            cache_updated = True
            # Optionally log the error: print(f"\n⚠️ Finnhub API error for {symbol}: {e}")
        
        # Respect Finnhub's free tier rate limit (60 calls/minute)
        time.sleep(1.1)
    
    if cache_updated:
        save_market_cap_cache(market_cap_cache)
        print(f"\n💾 Updated market cap cache with {len(market_cap_cache)} entries.")
    
    print() # Newline after progress bar
    return filtered_symbols


# --- % Return over Period using Alpaca ---
def get_return_for_period(symbol, years):
    """
    Calculates the percentage return for a symbol over a given number of years using Alpaca's API.
    """
    try:
        # Define the date range for the historical data request
        end_date = datetime.now(timezone.utc)
        start_date = end_date - relativedelta(years=years)

        # Create the request object for Alpaca
        request_params = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame.Day,
            start=start_date,
            end=end_date
        )

        # Fetch the data
        bars_df = alpaca_data_client.get_stock_bars(request_params).df
        
        # The dataframe is multi-indexed by (symbol, timestamp), reset it
        bars_df = bars_df.reset_index(level=0)

        if bars_df is None or len(bars_df) < 2:
            return None
            
        # Get the first available opening price and the last available closing price
        open_price = float(bars_df.iloc[0]['open'])
        close_price = float(bars_df.iloc[-1]['close'])
        
        if open_price == 0:
            return None # Avoid division by zero
            
        return ((close_price / open_price) - 1) * 100
        
    except Exception as e:
        # This can happen if a stock is new and has no data for the requested period
        # print(f"\n⚠️ Could not get return for {symbol} ({years}Y): {e}")
        return None


# --- Main Screener Logic ---
def run_screener():
    """
    Main function to execute the stock screening process.
    """
    all_us_symbols = get_us_stock_symbols()
    if not all_us_symbols:
        return

    # Prefilter for large-cap stocks to reduce the number of historical data requests
    # Market cap is in millions, so 100,000 = $100 Billion
    large_cap_symbols = prefilter_by_market_cap(all_us_symbols, min_cap_millions=10000)
    if not large_cap_symbols:
        print("\n❌ No stocks passed the market cap filter.")
        return

    print(f"\n✅ Found {len(large_cap_symbols)} stocks with market cap > $10B. Analyzing returns...")

    qualified_stocks = []
    total = len(large_cap_symbols)
    for i, symbol in enumerate(large_cap_symbols, 1):
        print_progress(i, total, prefix="📈 Return Analysis")
        
        try:
            # Fetch returns for multiple periods
            r10 = get_return_for_period(symbol, 10)
            r5 = get_return_for_period(symbol, 5)
            r3 = get_return_for_period(symbol, 3)
            
            # Skip if data for any period is unavailable
            if r10 is None or r5 is None or r3 is None:
                continue
            
            # Check if the stock meets all return criteria
            if (r10 > RETURNS_CRITERIA[10] and
                r5 > RETURNS_CRITERIA[5] and
                r3 > RETURNS_CRITERIA[3]):
                
                qualified_stocks.append({
                    "Symbol": symbol,
                    "Return 3Y (%)": f"{r3:.2f}",
                    "Return 5Y (%)": f"{r5:.2f}",
                    "Return 10Y (%)": f"{r10:.2f}",
                })
        except Exception as e:
            # Catch any unexpected errors during processing
            print(f"\n⚠️ Error processing {symbol}: {e}")
            continue
        
        # Respect Alpaca's data API rate limit (200 calls/min)
        # 3 calls per symbol, so a small delay is prudent
        time.sleep(0.5)

    print() # Clean newline after progress bar
    
    # --- Display Results ---
    if qualified_stocks:
        print(f"\n🏆 Found {len(qualified_stocks)} qualified stocks that met all criteria:")
        results_df = pd.DataFrame(qualified_stocks)
        print(results_df.to_string(index=False))
    else:
        print("\n❌ No stocks met all the specified return criteria.")


if __name__ == "__main__":
    # To run this, you need to install the required libraries:
    # pip install alpaca-trade-api pandas requests python-dateutil
    run_screener()
