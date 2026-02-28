import pandas as pd
from backtesting import Backtest, Strategy
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv
import os
import sys

# 🔐 Cargar claves desde alpkey.env
load_dotenv("alpkey.env")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = "https://paper-api.alpaca.markets"

# Validar que las claves se hayan cargado
if not API_KEY or not SECRET_KEY:
    print("Error: Alpaca API keys not found in alpkey.env.", file=sys.stderr)
    sys.exit(1)

# 📈 Inicializa la API de Alpaca (usando alpaca-trade-api para el ajuste de datos)
api = tradeapi.REST(
    key_id=API_KEY,
    secret_key=SECRET_KEY,
    base_url=BASE_URL,
    api_version="v2"
)

#  tickers a analizar
tickers = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]

# 🗓️ Parámetros de descarga
start_date = "2015-01-01"
end_date = "2024-01-01"

# 🧠 Estrategia Buy and Hold (definida una vez fuera del bucle)
class BuyAndHold(Strategy):
    def init(self):
        pass

    def next(self):
        # Si no hay posición, compra con todo el capital disponible
        if not self.position:
            self.buy()

# --- Bucle de Backtesting ---
all_results = []

for symbol in tickers:
    print(f"\n--- Analizando {symbol} ---")
    try:
        # 📥 Descarga datos históricos semanales, ajustados por splits y dividendos
        # El parámetro 'adjustment' es la clave para manejar los splits correctamente.
        bars = api.get_bars(
            symbol,
            tradeapi.TimeFrame.Week,
            start=start_date,
            end=end_date,
            adjustment='all'  # 'all' = split and dividend adjusted
        ).df

        if bars.empty:
            print(f"No data found for {symbol} in the given date range.")
            continue

        # 🧼 Formatea para backtesting.py
        df = bars[["open", "high", "low", "close", "volume"]].copy()
        df.columns = ["Open", "High", "Low", "Close", "Volume"]
        # backtesting.py funciona mejor con datetimes "naive" (sin zona horaria)
        df.index = df.index.tz_convert('America/New_York').tz_localize(None)

        # 🚀 Ejecuta el backtest
        bt = Backtest(df, BuyAndHold, cash=10000, commission=.002)
        stats = bt.run()

        # Almacena las estadísticas clave
        all_results.append({
            "Symbol": symbol,
            "Return [%]": stats["Return [%]"],
            "Buy & Hold Return [%]": stats["Buy & Hold Return [%]"],
            "Max. Drawdown [%]": stats["Max. Drawdown [%]"],
            "Sharpe Ratio": stats["Sharpe Ratio"],
            "# Trades": stats["# Trades"]
        })

    except Exception as e:
        print(f"Error processing {symbol}: {e}", file=sys.stderr)

# --- Mostrar Resultados Consolidados ---
if all_results:
    results_df = pd.DataFrame(all_results)
    print("\n\n--- Resultados Consolidados del Backtest ---")
    print(results_df.to_string(index=False))
else:
    print("\nNo se pudo completar el backtest para ningún ticker.")
