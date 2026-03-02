import pandas as pd
from backtesting import Backtest, Strategy
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv
import os
import sys

# 🔐 Cargar claves desde alpkey.env
load_dotenv("alpkey.env")

API_KEY = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET_KEY")
BASE_URL = "https://paper-api.alpaca.markets"

# Validar que las claves se hayan cargado
if not API_KEY or not SECRET_KEY:
    print("Error: Alpaca API keys not found.", file=sys.stderr)
    print("Please create an 'alpkey.env' file in the same directory with your keys:", file=sys.stderr)
    print("APCA_API_KEY_ID='YOUR_KEY_ID'", file=sys.stderr)
    print("APCA_API_SECRET_KEY='YOUR_SECRET_KEY'", file=sys.stderr)
    sys.exit(1)

# 📈 Inicializa la API de Alpaca
api = tradeapi.REST(
    key_id=API_KEY,
    secret_key=SECRET_KEY,
    base_url=BASE_URL,
    api_version="v2"
)

# 🗓️ Parámetros de descarga
symbol = "GOOGL"
  # Cambia por el ticker que desees
start_date = "2020-09-18"
end_date = "2025-09-18"

# 📥 Descarga datos históricos diarios
try:
    bars = api.get_bars(symbol, tradeapi.TimeFrame.Day, start=start_date, end=end_date).df
except Exception as e:
    print(f"Error downloading data for {symbol}: {e}", file=sys.stderr)
    sys.exit(1)

df = bars.copy()

# 🧼 Formatea para backtesting.py
df = df[["open", "high", "low", "close", "volume"]]
df.columns = ["Open", "High", "Low", "Close", "Volume"]
# The index from Alpaca is timezone-aware. backtesting.py works best with naive datetimes.
df.index = df.index.tz_convert('America/New_York').tz_localize(None)

# 🧠 Estrategia Buy and Hold
class BuyAndHold(Strategy):
    def init(self):
        pass

    def next(self):
        if not self.position:
            self.buy()

# 🚀 Ejecuta el backtest
bt = Backtest(df, BuyAndHold, cash=10000, commission=0.002)
results = bt.run()
print(results)
bt.plot()
