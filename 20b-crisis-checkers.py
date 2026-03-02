import pandas as pd
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import time
import os
import json
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import logging
from dotenv import load_dotenv
load_dotenv()


# --- File Path for Market Cap Data ---
# This is the full path to your local JSON cache file.
MARKET_CAP_FILE_PATH = r"C:\Users\USERNAME\OneDrive\Documents\DS - Coding - Python\Stocks\Alpaca\Backup marketcap\market_cap_cache.json"

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'
)

# --- Credentials ---
# IMPORTANT: Replace the placeholders with your real keys.
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')

# --- Crisis Definitions ---
# Note: Future-dated crises will be automatically skipped to prevent errors.
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

# --- API Client ---
try:
    alpaca_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    logging.info("✅ Successfully initialized Alpaca data client.")
except Exception as e:
    logging.error(f"❌ Error initializing Alpaca client: {e}")
    logging.error("Please ensure your Alpaca API keys are correct and have the necessary permissions.")
    quit()


# --- Progress Bar Function ---
def print_progress(iteration, total, prefix='', length=30):
    """Prints a simple progress bar to the console."""
    percent = int(100 * iteration / total)
    filled = int(length * iteration / total)
    bar = '#' * filled + '-' * (length - filled)
    print(f'\r{prefix} [{bar}] {percent}% ({iteration}/{total})', end='', flush=True)


# --- Load and Filter Symbols from JSON Cache ---
def get_large_cap_symbols_from_json(file_path, min_cap_billions):
    """
    Parses a local JSON file to extract stock symbols with a market cap
    greater than the specified minimum.

    Args:
        file_path (str): The full path to the market_cap_cache.json file.
        min_cap_billions (float): The minimum market cap in billions (e.g., 21).

    Returns:
        list: A list of stock symbols that meet the criteria.
    """
    logging.info(f"Loading market cap data from: {file_path}")
    min_cap_millions = min_cap_billions * 1000

    try:
        with open(file_path, 'r') as f:
            market_data = json.load(f)
    except FileNotFoundError:
        logging.error(f"FATAL: The file was not found at the specified path.")
        return []
    except json.JSONDecodeError:
        logging.error(f"FATAL: The file at {file_path} is not a valid JSON file.")
        return []

    stock_data = market_data.get('data', {})
    if not stock_data:
        logging.warning("No 'data' key found in the JSON file.")
        return []

    filtered_symbols = [
        symbol for symbol, cap in stock_data.items()
        if cap is not None and isinstance(cap, (int, float)) and cap >= min_cap_millions
    ]
    
    if not filtered_symbols:
        logging.warning(f"No symbols found with a market cap over ${min_cap_billions}B.")
    
    return filtered_symbols


# --- % Return for a Specific Date Range ---
def get_return_for_date_range(symbol, start_date, end_date):
    """
    Fetches historical data from Alpaca and calculates the percentage return
    for a given symbol over a specific date range.
    """
    # Adjust end_date to yesterday if it's in the future
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
        
        # Ensure there's enough data to calculate a return
        if bars_df.empty or len(bars_df) < 2:
            logging.debug(f"Insufficient data for {symbol} in the given range.")
            return None
        
        # The dataframe from alpaca-py can have a multi-index ('symbol', 'timestamp')
        # We need to handle this to access columns directly.
        bars_df = bars_df.reset_index(level=0, drop=True) # Drop 'symbol' index level
        
        open_price = float(bars_df.iloc[0]['open'])
        close_price = float(bars_df.iloc[-1]['close'])
        
        if open_price == 0:
            logging.warning(f"Open price is zero for {symbol}. Cannot calculate return.")
            return None
            
        return ((close_price / open_price) - 1) * 100
    except Exception as e:
        logging.error(f"Could not fetch/process data for {symbol}: {e}")
        return None

# --- Crisis Analysis Logic ---
def analyze_crisis_performance(symbols, crises):
    """
    Analyzes stock performance across defined crisis periods.
    """
    all_results = {}
    
    # Filter out crises that are in the future
    past_crises = {
        name: dates for name, dates in crises.items() 
        if dates["start"] < datetime.now()
    }
    
    if not past_crises:
        logging.warning("No past crisis periods defined for analysis. Exiting analysis.")
        return {}

    for crisis_name, dates in past_crises.items():
        logging.info(f"\n--- Analyzing Crisis: {crisis_name} ---")
        crisis_results = []
        
        # Define the periods for analysis
        before_start = dates["start"] - relativedelta(months=3)
        after_end = dates["end"] + relativedelta(months=6)

        total_symbols = len(symbols)
        for i, symbol in enumerate(symbols, 1):
            print_progress(i, total_symbols, prefix=f"📈 Analyzing {crisis_name}")
            
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
            
            # Rate limiting to be respectful to the API
            time.sleep(0.3) 
        
        print() # Newline after the progress bar finishes
        
        if crisis_results:
            results_df = pd.DataFrame(crisis_results)
            results_df = results_df.sort_values(by="After (%)", ascending=False).reset_index(drop=True)
            all_results[crisis_name] = results_df
        else:
            logging.warning(f"No symbols had complete data for the '{crisis_name}' period.")

    return all_results

# --- Main Screener Logic ---
def run_screener():
    """
    Main function to execute the screener workflow.
    """
    large_cap_symbols = get_large_cap_symbols_from_json(
        file_path=MARKET_CAP_FILE_PATH, 
        min_cap_billions=21
    )
    
    if not large_cap_symbols:
        logging.error("No stocks passed the market cap filter. Exiting.")
        return

    logging.info(f"Found {len(large_cap_symbols)} stocks with market cap > $21B. Analyzing crisis performance...")

    final_results = analyze_crisis_performance(large_cap_symbols, CRISIS_PERIODS)

    print("\n\n--- CRISIS PERFORMANCE ANALYSIS COMPLETE ---")
    if not final_results:
        print("No stocks had complete data for any of the analysis periods.")
        return

    pd.set_option('display.float_format', '{:.2f}'.format)
    for crisis_name, df in final_results.items():
        print(f"\n\n🏆 Top 15 Stock Recoveries for: {crisis_name}")
        print("-----------------------------------------------------")
        print(df.head(15).to_string(index=False))


if __name__ == "__main__":
    run_screener()
