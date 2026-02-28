import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
from dotenv import load_dotenv
import os
import sys

# 🔑 Alpaca API credentials
load_dotenv("alpkey.env")
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

# ⏱️ Period to analyze
start_date = "2025-08-01"
end_date = "2025-09-03"

# 📈 List of tickers to analyze
tickers = ["IAUM","CCJ","TFPM","ARGX","GLD"]

# Validate keys
if not API_KEY or not SECRET_KEY:
    print("Error: Alpaca API keys not found in alpkey.env.", file=sys.stderr)
    sys.exit(1)

# ✅ Initialize Alpaca client
client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

def get_price_data(ticker):
    try:
        request_params = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame.Day,
            start=datetime.fromisoformat(start_date),
            end=datetime.fromisoformat(end_date)
        )
        bars = client.get_stock_bars(request_params).df

        # Drop symbol level if exists (happens when multiple symbols are returned)
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.droplevel(0)

        bars = bars[['close']].rename(columns={'close': ticker})
        return bars
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None

# 📥 Fetch all ticker data
price_data_list = []

for ticker in tickers:
    df = get_price_data(ticker)
    if df is not None:
        price_data_list.append(df)

# 🧱 Abort early if nothing was fetched
if not price_data_list:
    raise ValueError("No data was fetched. Please check ticker symbols and API access.")

# 📊 Merge all price data (on date)
all_prices = pd.concat(price_data_list, axis=1, join="outer")

# 📈 Compute daily % returns
returns = all_prices.pct_change()

# 🧠 Drop rows with all NaNs (no data that day)
returns = returns.dropna(how='all')

# 🧮 Compute correlation matrix
correlation_matrix = returns.corr()

# 📋 Display matrix
print("\nCorrelation matrix of daily % returns:")
print(correlation_matrix)

# 🔥 Visualize
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Correlation Matrix of Stock Price Performance")
plt.tight_layout()
plt.show()
