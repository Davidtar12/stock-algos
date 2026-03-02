import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta # You may need to run: pip install python-dateutil
from dotenv import load_dotenv
import os
load_dotenv()

# Make sure to install the alpaca-trade-api library: pip install alpaca-trade-api
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import REST, TimeFrame

# --- Configuration for Alpaca ---
API_KEY = os.getenv('ALPACA_API_KEY')
SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
BASE_URL = "https://paper-api.alpaca.markets"

# --- Initialize Alpaca API ---
def initialize_alpaca():
    """Establishes connection to the Alpaca API."""
    try:
        api = REST(API_KEY, SECRET_KEY, base_url=BASE_URL)
        account = api.get_account()
        print(f"✅ Successfully connected to Alpaca account: {account.id}")
        return api
    except Exception as e:
        print(f"❌ Failed to initialize Alpaca API: {e}")
        quit()

# --- Get daily data for a symbol in a given time range ---
# This function remains the same, but will now be called less frequently.
def get_daily_data(api, symbol, start_date, end_date):
    """Fetches daily historical bar data from Alpaca."""
    try:
        start_iso = start_date.strftime('%Y-%m-%d')
        end_iso = end_date.strftime('%Y-%m-%d')
        bars_df = api.get_bars(symbol, TimeFrame.Day, start_iso, end_iso, adjustment='raw').df
        if bars_df.empty:
            return pd.DataFrame()
        bars_df.reset_index(inplace=True)
        bars_df.rename(columns={'timestamp': 'time'}, inplace=True)
        bars_df['time'] = bars_df['time'].dt.tz_localize(None)
        return bars_df
    except Exception as e:
        return pd.DataFrame()

# --- Analyze returns for specific days of the month (OPTIMIZED) ---
def analyze_days(api, symbol, start_year, start_month=1, end_year=None):
    """
    Calculates the 10-day return preceding each day from the 1st to the 17th.
    This version is OPTIMIZED to make one API call per month instead of per day.
    """
    if end_year is None:
        end_year = datetime.now().year

    results = []

    for year in range(start_year, end_year + 1):
        month_range_start = start_month if year == start_year else 1
        
        for month in range(month_range_start, 13):
            # Define the window for the entire month's analysis
            start_of_month = datetime(year, month, 1)
            end_of_month = start_of_month + relativedelta(months=1)
            
            # Fetch data once for the whole month, with a 30-day buffer for lookback
            # This is the single API call for the month.
            df_month = get_daily_data(api, symbol, start_of_month - timedelta(days=30), end_of_month)

            if df_month.empty:
                continue # Skip to next month if no data was returned

            # Now, loop through the days and use the data we already downloaded
            for day in range(1, 18):
                try:
                    test_date = datetime(year, month, day)
                    if test_date > datetime.now():
                        continue

                    # Slice the pre-fetched DataFrame instead of making a new API call
                    df_slice = df_month[df_month['time'] < test_date].sort_values('time')

                    if len(df_slice) < 10:
                        continue

                    # The calculation logic remains the same
                    open_price = df_slice.iloc[-10]['open']
                    close_price = df_slice.iloc[-1]['close']
                    ret = (close_price / open_price - 1) * 100

                    results.append({
                        'Year': year,
                        'Month': month,
                        'Test_Day': day,
                        'Return_%': ret
                    })
                except Exception as e:
                    continue

    return pd.DataFrame(results)

# --- Main Execution ---
# This part remains the same.
def main():
    """Main function to run the analysis."""
    api = initialize_alpaca()

    symbol = input("Enter ticker symbol (e.g., AAPL, TSLA, RDDT): ").upper()

    try:
        start_year_input = int(input(f"Enter the start year for analysis (e.g., 2024 for RDDT): "))
    except ValueError:
        print("Invalid year. Defaulting to the current year.")
        start_year_input = datetime.now().year

    try:
        start_month_input = int(input(f"Enter the start month for {start_year_input} (e.g., 3 for March): "))
        if not 1 <= start_month_input <= 12:
             print("Invalid month number. Defaulting to 1 (January).")
             start_month_input = 1
    except ValueError:
        print("Invalid input. Defaulting to 1 (January).")
        start_month_input = 1

    print(f"\nAnalyzing {symbol} from {start_month_input}/{start_year_input} onwards...")
    df = analyze_days(api, symbol, start_year=start_year_input, start_month=start_month_input)

    if df.empty:
        print("No data found for the specified symbol and date range.")
        return

    summary = df.groupby('Test_Day')['Return_%'].mean().sort_values(ascending=False)

    print("\n--- Analysis Complete ---")
    print("\nAverage 10-day return BEFORE days 1 to 17 of each month:")
    print(summary.to_string())

    if not summary.empty:
        best_day = summary.idxmax()
        print(f"\n📈 **Best average return is observed in the 10 trading days before day {best_day} of the month.**")

    file_name = f"{symbol}_day_return_analysis.csv"
    df.to_csv(file_name, index=False)
    print(f"\n💾 Saved detailed results to {file_name}")

if __name__ == '__main__':
    main()
