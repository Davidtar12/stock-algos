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
# Get data & apply indicators
# -----------------------------
data = bfs._get_data_for_bt(timeframe, symbol, 400)

# Volume Surge Component
volume_length = 20
volume_multiplier = 1.5
data['avg_volume'] = ta.sma(data['Volume'], length=volume_length)
data['volume_surge'] = data['Volume'] > volume_multiplier * data['avg_volume']

# Momentum Component (RSI)
rsi_length = 14
rsi_bullish_level = 50
data['rsi'] = ta.rsi(data['Close'], length=rsi_length)
data['rsi_rising'] = (data['rsi'] > rsi_bullish_level) & (data['rsi'] > data['rsi'].shift(1))

# MACD Confirmation
fast_length = 8
slow_length = 21
macd_length = 5
macd = ta.macd(data['Close'], fast=fast_length, slow=slow_length, signal=macd_length)
data['macd'] = macd[f'MACD_{fast_length}_{slow_length}_{macd_length}']
data['signal'] = macd[f'MACDs_{fast_length}_{slow_length}_{macd_length}']
data['macd_bullish'] = data['macd'] > data['signal']

# Trend Filter (SMA20 and SMA50)
data['SMA_20'] = ta.sma(data['Close'], length=20)
data['SMA_50'] = ta.sma(data['Close'], length=50)
data['trend_filter'] = (data['Close'] > data['SMA_20']) & (data['SMA_20'] > data['SMA_50'])

print(data.tail())

# -----------------------------
# Strategy class
# -----------------------------
class QuallamagieBullish(Strategy):
    def init(self):
        pass

    def next(self):
        # Check for NaN values in indicators
        if (np.isnan(self.data.volume_surge[-1]) or 
            np.isnan(self.data.rsi_rising[-1]) or 
            np.isnan(self.data.macd_bullish[-1]) or 
            np.isnan(self.data.trend_filter[-1])):
            return

        # Entry condition (at least 3 out of 4 conditions)
        conditions = [
            self.data.volume_surge[-1],
            self.data.rsi_rising[-1],
            self.data.macd_bullish[-1],
            self.data.trend_filter[-1]
        ]
        buy_signal = sum(conditions) >= 3  # Fixed: Complete sum expression

        print(f"Close={self.data.Close[-1]:.2f} | "
              f"VolumeSurge={self.data.volume_surge[-1]} | "
              f"RSI={self.data.rsi[-1]:.2f} | "
              f"MACDBullish={self.data.macd_bullish[-1]} | "
              f"TrendFilter={self.data.trend_filter[-1]} | "
              f"BuySignal={buy_signal}")

        if buy_signal:
            if not self.position:
                self.buy()
        else:
            if self.position:
                self.position.close()

# -----------------------------
# Backtest
# -----------------------------
bt = Backtest(data, QuallamagieBullish, cash=10_000, commission=0.002, exclusive_orders=True)

results = bt.run()
print(results)
bt.plot()