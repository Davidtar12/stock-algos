import pandas as pd
import numpy as np
from datetime import datetime, timedelta
# Make sure to install the alpaca-trade-api library: pip install alpaca-trade-api
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import REST, TimeFrame
from dotenv import load_dotenv
import os
import sys

# --- Configuration for Alpaca ---
load_dotenv("alpkey.env")
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = "https://paper-api.alpaca.markets" 

# --- Initialize Alpaca API ---
def initialize_alpaca():
    """Establishes connection to the Alpaca API."""
    try:
        if not API_KEY or not SECRET_KEY:
            raise ValueError("Alpaca API keys not found in environment variables.")
        api = REST(API_KEY, SECRET_KEY, base_url=BASE_URL)
        # Check if the connection is successful by fetching account info
        account = api.get_account()
        print(f"✅ Successfully connected to Alpaca account: {account.id}")
        return api
    except Exception as e:
        print(f"❌ Failed to initialize Alpaca API: {e}")
        sys.exit(1)

# --- Get daily data for a symbol in a given time range ---
def get_daily_data(api, symbol, start_date, end_date):
    """Fetches daily historical bar data from Alpaca."""
    try:
        # Format dates to ISO 8601 string format required by Alpaca
        start_iso = start_date.strftime('%Y-%m-%d')
        end_iso = end_date.strftime('%Y-%m-%d')
        
        # Fetch the data using the Alpaca API and return it as a pandas DataFrame
        bars_df = api.get_bars(symbol, TimeFrame.Day, start_iso, end_iso, adjustment='raw').df
        
        if bars_df.empty:
            return pd.DataFrame()

        # The DataFrame index is a timezone-aware timestamp. 
        # Reset index to get a 'timestamp' column.
        bars_df.reset_index(inplace=True)
        
        # Rename 'timestamp' to 'time' for compatibility with the original script logic
        bars_df.rename(columns={'timestamp': 'time'}, inplace=True)

        # Convert the 'time' column to timezone-naive datetime objects to allow comparisons
        bars_df['time'] = bars_df['time'].dt.tz_localize(None)
        
        return bars_df

    except Exception as e:
        # Handles cases where the symbol is invalid or no data is available
        # print(f"Could not fetch data for {symbol}: {e}") # Uncomment for debugging
        return pd.DataFrame()

# --- Analyze returns for specific days of the month ---
def analyze_days(api, symbol, start_year=2010, end_year=None):
    """
    Calculates the 10-day return preceding each day from the 1st to the 17th
    of every month within the specified year range.
    """
    if end_year is None:
        end_year = datetime.now().year

    results = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            for day in range(1, 18):
                try:
                    test_date = datetime(year, month, day)
                    if test_date > datetime.now():
                        continue

                    # Define a 14-day window to ensure we get at least 10 trading days
                    start_window = test_date - timedelta(days=20) 
                    end_window = test_date

                    df = get_daily_data(api, symbol, start_window, end_window)
                    
                    if df.empty or len(df) < 10:
                        continue

                    # Filter data to be strictly before the test day and sort by time
                    df = df[df['time'] < test_date].sort_values('time')

                    if len(df) < 10:
                        continue

                    # Calculate the return over the last 10 available trading days
                    open_price = df.iloc[-10]['open']
                    close_price = df.iloc[-1]['close']
                    ret = (close_price / open_price - 1) * 100

                    results.append({
                        'Year': year,
                        'Month': month,
                        'Test_Day': day,
                        'Return_%': ret
                    })
                except Exception as e:
                    # Silently continue if a date is invalid (e.g., Feb 30) or another error occurs
                    continue
    
    return pd.DataFrame(results)

# --- Main Execution ---
def main():
    """Main function to run the analysis."""
    api = initialize_alpaca()
    
    symbol = input("Enter ticker symbol (e.g., AAPL, TSLA): ").upper()
    
    print(f"\nAnalyzing {symbol}...")
    df = analyze_days(api, symbol)
    
    if df.empty:
        print("No data found for the specified symbol and date range.")
        return

    # Group by the test day and calculate the average return
    summary = df.groupby('Test_Day')['Return_%'].mean().sort_values(ascending=False)
    
    print("\n--- Analysis Complete ---")
    print("\nAverage 10-day return BEFORE days 1 to 17 of each month:")
    print(summary.to_string())

    best_day = summary.idxmax()
    print(f"\n📈 **Best average return is observed in the 10 trading days before day {best_day} of the month.**")

    # Save the full results to a CSV file
    file_name = f"{symbol}_day_return_analysis.csv"
    df.to_csv(file_name, index=False)
    print(f"\n💾 Saved detailed results to {file_name}")

if __name__ == '__main__':
    main()