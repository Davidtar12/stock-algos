import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import pandas_ta as ta
from backtesting import Backtest, Strategy
from Easy_Trading import Basic_funcs

# -----------------------------
# Connect to Admirals
# -----------------------------
import os
nombre = int(os.getenv("ADMIRALS_LOGIN", "0"))
clave = os.getenv("ADMIRALS_PASSWORD")
servidor = os.getenv("ADMIRALS_SERVER", "AdmiralsSC-Demo")
path = r"C:\\Program Files\\Admirals SC MT5 Terminal\\terminal64.exe"

bfs = Basic_funcs(nombre, clave, servidor, path)

symbol = "#VOO-T"
timeframe = mt5.TIMEFRAME_D1

# -----------------------------
# Get data & apply pandas_ta
# -----------------------------
data = bfs._get_data_for_bt(timeframe, symbol, 400)

# Add SMA columns using pandas_ta
data['SMA_50'] = ta.sma(data['Close'], length=50)
data['SMA_200'] = ta.sma(data['Close'], length=200)

print(data.tail())

# -----------------------------
# Strategy class: just use the columns!
# -----------------------------
class MinerviniVCP(Strategy):
    def init(self):
        # This method can be left empty if no initialization is needed
        pass

    def next(self):
        close = self.data.Close[-1]
        ma50 = self.data.SMA_50[-1]
        ma200 = self.data.SMA_200[-1]

        if np.isnan(ma50) or np.isnan(ma200):
            return

        uptrend = close > ma200 and ma50 > ma200 and close > ma50

        print(f"Close={close:.2f} | MA50={ma50:.2f} | MA200={ma200:.2f} | Uptrend={uptrend}")

        if uptrend:
            if not self.position:
                self.buy()
        else:
            if self.position:
                self.position.close()

# -----------------------------
# Backtest
# -----------------------------
bt = Backtest(data, MinerviniVCP, cash=10_000, commission=0.002, exclusive_orders=True)

results = bt.run()
print(results)
bt.plot()
