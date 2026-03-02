import pandas as pd
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import time
import os
import requests
import json
from alpaca_trade_api.rest import REST
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import logging
from dotenv import load_dotenv
load_dotenv()


# --- Configuración de Rutas de Archivo ---
# Los archivos de caché se guardarán en esta carpeta específica.
TARGET_DIRECTORY = r"C:\Users\USERNAME\OneDrive\Documents\DS - Coding - Python\Stocks\Alpaca"
# Crea el directorio si no existe para evitar errores.
os.makedirs(TARGET_DIRECTORY, exist_ok=True)

# Define las rutas completas para los archivos de caché.
CACHE_FILE = os.path.join(TARGET_DIRECTORY, "us_stock_symbols_cache.txt")
MARKET_CAP_CACHE_FILE = os.path.join(TARGET_DIRECTORY, "market_cap_cache.json")


# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'
)

# --- Credentials ---
# IMPORTANTE: Reemplaza los placeholders con tus claves reales.
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')

# --- Crisis Definitions ---
CRISIS_PERIODS = {
    "COVID-19 Crash": {
        "start": datetime(2020, 2, 20),
        "end": datetime(2020, 4, 7)
    },
    "Deepseek AI Impact": {
        "start": datetime(2025, 2, 1),
        "end": datetime(2025, 3, 1)
    },
    "2025 Tariff Crisis": {
        "start": datetime(2025, 4, 1),
        "end": datetime(2025, 5, 1)
    },
    "2025 Yen Carry Trade Unwind": {
        "start": datetime(2025, 5, 15),
        "end": datetime(2025, 6, 15)
    }
}

CACHE_EXPIRY_DAYS = 7

# --- API Clients ---
try:
    alpaca_api = REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')
    alpaca_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
except Exception as e:
    logging.error(f"❌ Error initializing Alpaca clients: {e}")
    quit()


# --- Progress Bar Function ---
def print_progress(iteration, total, prefix='', length=20):
    percent = int(100 * iteration / total)
    filled = int(length * iteration / total)
    bar = '#' * filled + '-' * (length - filled)
    print(f'\r{prefix} [{bar}] {percent}% ({iteration}/{total})', end='', flush=True)


# --- Fetch US Stocks from Alpaca ---
def get_us_stock_symbols():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            symbols = [line.strip() for line in f]
            logging.info(f"Loaded {len(symbols)} symbols from local cache: {CACHE_FILE}")
            return symbols
    else:
        logging.info("Fetching symbols from Alpaca API...")
        try:
            assets = alpaca_api.list_assets(status='active', asset_class='us_equity')
            symbols = [
                asset.symbol for asset in assets
                if asset.tradable and asset.exchange in ['NASDAQ', 'NYSE', 'ARCA', 'BATS']
            ]
            with open(CACHE_FILE, "w") as f:
                for symbol in symbols:
                    f.write(symbol + "\n")
            logging.info(f"Cached {len(symbols)} symbols to {CACHE_FILE}")
            return symbols
        except Exception as e:
            logging.error(f"Failed to retrieve symbols from Alpaca: {e}")
            return []


# --- Load and Save Market Cap Cache ---
def load_market_cap_cache():
    if not os.path.exists(MARKET_CAP_CACHE_FILE):
        return {}
    try:
        with open(MARKET_CAP_CACHE_FILE, "r") as f:
            cache = json.load(f)
        cache_date = datetime.fromisoformat(cache.get("last_updated", "1970-01-01"))
        if (datetime.now() - cache_date).days > CACHE_EXPIRY_DAYS:
            logging.warning("Market cap cache expired, will refresh.")
            return {}
        logging.info(f"Loaded market cap cache with {len(cache.get('data', {}))} entries from {MARKET_CAP_CACHE_FILE}")
        return cache.get("data", {})
    except (json.JSONDecodeError, IOError) as e:
        logging.error(f"Could not load market cap cache: {e}")
        return {}

def save_market_cap_cache(cache_data):
    cache = {
        "last_updated": datetime.now().isoformat(),
        "data": cache_data
    }
    with open(MARKET_CAP_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)
    logging.info(f"Market cap cache saved to {MARKET_CAP_CACHE_FILE}")


# --- Market Cap Filter via Finnhub ---
def prefilter_by_market_cap(symbols, min_cap_millions):
    market_cap_cache = load_market_cap_cache()
    filtered_symbols = []
    cache_updated = False
    total = len(symbols)
    
    logging.info(f"Starting market cap filter for {total} symbols...")

    for i, symbol in enumerate(symbols, 1):
        logging.debug(f"Processing [{i}/{total}]: {symbol}")

        if not symbol or not isinstance(symbol, str) or '.' in symbol:
            logging.warning(f"Skipping invalid symbol format: {symbol}")
            continue

        if symbol in market_cap_cache:
            cap = market_cap_cache[symbol]
            if cap and cap > min_cap_millions:
                logging.info(f"✅ PASSED (from cache): {symbol} | Market Cap: ${cap:,.0f}M")
                filtered_symbols.append(symbol)
            continue
        
        try:
            url = f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={FINNHUB_API_KEY}"
            response = requests.get(url, timeout=10)
            response.raise_for_status() 
            
            profile = response.json()
            if not profile:
                 logging.warning(f"No profile data returned for {symbol}. Skipping.")
                 market_cap_cache[symbol] = None
                 cache_updated = True
                 time.sleep(1.2)
                 continue

            cap = profile.get("marketCapitalization")
            market_cap_cache[symbol] = cap
            cache_updated = True
            
            if cap and cap > min_cap_millions:
                logging.info(f"✅ PASSED: {symbol} | Market Cap: ${cap:,.0f}M")
                filtered_symbols.append(symbol)
            elif cap is not None:
                logging.debug(f"SKIPPED: {symbol} cap ${cap:,.0f}M is below threshold.")
            else:
                 logging.warning(f"No market cap data in profile for {symbol}.")

        except requests.RequestException as e:
            logging.error(f"🚨 ERROR on {symbol}: {e}")
            market_cap_cache[symbol] = None
            cache_updated = True
            
        time.sleep(1.2)
    
    if cache_updated:
        save_market_cap_cache(market_cap_cache)
        
    return filtered_symbols


# --- % Return for a Specific Date Range ---
def get_return_for_date_range(symbol, start_date, end_date):
    if end_date > datetime.now():
        end_date = datetime.now() - timedelta(days=1)

    try:
        request_params = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame.Day,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d')
        )
        bars_df = alpaca_data_client.get_stock_bars(request_params).df
        if bars_df.empty or len(bars_df) < 2:
            return None
        
        bars_df = bars_df.reset_index(level=0)
        open_price = float(bars_df.iloc[0]['open'])
        close_price = float(bars_df.iloc[-1]['close'])
        
        if open_price == 0:
            return None
        return ((close_price / open_price) - 1) * 100
    except Exception:
        return None

# --- Crisis Analysis Logic ---
def analyze_crisis_performance(symbols, crises):
    all_results = {}
    
    for crisis_name, dates in crises.items():
        logging.info(f"--- Analyzing Crisis: {crisis_name} ---")
        crisis_results = []
        
        before_start = dates["start"] - relativedelta(months=3)
        after_end = dates["end"] + relativedelta(months=6)

        total = len(symbols)
        for i, symbol in enumerate(symbols, 1):
            print_progress(i, total, prefix=f"📈 Analyzing {crisis_name}")
            
            before_return = get_return_for_date_range(symbol, before_start, dates["start"] - timedelta(days=1))
            during_return = get_return_for_date_range(symbol, dates["start"], dates["end"])
            after_return = get_return_for_date_range(symbol, dates["end"] + timedelta(days=1), after_end)
            
            if all(r is not None for r in [before_return, during_return, after_return]):
                crisis_results.append({
                    "Symbol": symbol,
                    "Before (%)": before_return,
                    "During (%)": during_return,
                    "After (%)": after_return,
                })
            
            time.sleep(0.35)
        
        print()
        
        if crisis_results:
            results_df = pd.DataFrame(crisis_results)
            results_df = results_df.sort_values(by="After (%)", ascending=False).reset_index(drop=True)
            all_results[crisis_name] = results_df

    return all_results

# --- Main Screener Logic ---
def run_screener():
    all_us_symbols = get_us_stock_symbols()
    if not all_us_symbols:
        return

    large_cap_symbols = prefilter_by_market_cap(all_us_symbols, min_cap_millions=10000)
    if not large_cap_symbols:
        logging.error("No stocks passed the market cap filter. Exiting.")
        return

    logging.info(f"Found {len(large_cap_symbols)} stocks with market cap > $10B. Analyzing crisis performance...")

    final_results = analyze_crisis_performance(large_cap_symbols, CRISIS_PERIODS)

    print("\n\n--- CRISIS PERFORMANCE ANALYSIS COMPLETE ---")
    if not final_results:
        print("No stocks had complete data for the analysis periods.")
        return

    pd.set_option('display.float_format', '{:.2f}'.format)
    for crisis_name, df in final_results.items():
        print(f"\n\n🏆 Top 15 Stock Recoveries for: {crisis_name}")
        print("-----------------------------------------------------")
        print(df.head(15).to_string(index=False))


if __name__ == "__main__":
    run_screener()
