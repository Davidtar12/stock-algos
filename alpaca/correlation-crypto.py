import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()


# 🔑 Alpaca API credentials
API_KEY = os.getenv('ALPACA_API_KEY')
SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')

# ⏱️ Date range
start_date = "2023-01-01"
end_date = "2025-08-01"

# 📈 Crypto pairs to analyze (quoted in USD)
tickers = ["BTC/USD", "ETH/USD", "SOL/USD", "AVAX/USD", "DOGE/USD", "ADA/USD","XRP/USD","LINK/USD","LTC/USD"]

# ✅ Initialize Crypto client
client = CryptoHistoricalDataClient(API_KEY, SECRET_KEY)

def get_crypto_data(ticker):
    try:
        request_params = CryptoBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame.Day,
            start=datetime.fromisoformat(start_date),
            end=datetime.fromisoformat(end_date)
        )
        bars = client.get_crypto_bars(request_params).df

        # Drop symbol level if MultiIndex
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.droplevel(0)

        bars = bars[['close']].rename(columns={'close': ticker})
        return bars
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None

# 📥 Fetch all data
price_data_list = []

for ticker in tickers:
    df = get_crypto_data(ticker)
    if df is not None:
        price_data_list.append(df)

if not price_data_list:
    raise ValueError("No data was fetched. Please check crypto symbols or API access.")

# 📊 Merge on datetime index
all_prices = pd.concat(price_data_list, axis=1, join="outer")

# 📈 Daily percentage returns
returns = all_prices.pct_change()
returns = returns.dropna(how='all')

# 🧮 Correlation matrix
correlation_matrix = returns.corr()

# 📋 Print matrix
print("\nCorrelation matrix of daily % returns:")
print(correlation_matrix)

# 🔥 Plot
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Crypto Correlation Matrix (Daily Returns)")
plt.tight_layout()
plt.show()
