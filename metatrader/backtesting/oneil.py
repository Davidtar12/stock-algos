import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy

# -----------------------------
# Connect to Admirals MT5
# -----------------------------
import os
nombre = int(os.getenv("ADMIRALS_LOGIN", "0"))
clave = os.getenv("ADMIRALS_PASSWORD")
servidor = os.getenv("ADMIRALS_SERVER", "AdmiralsSC-Demo")
path = r"C:\\Program Files\\Admirals SC MT5 Terminal\\terminal64.exe"

if not mt5.initialize(path, login=nombre, password=clave, server=servidor):
    print("Failed to connect:", mt5.last_error())
    quit()
else:
    print("Connected to Admirals MT5")

# -----------------------------
# Get data
# -----------------------------
symbol = "#AAPL-T"
timeframe = mt5.TIMEFRAME_D1
bars = 400  # Enough candles for lookbacks

rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
data = pd.DataFrame(rates)
data['time'] = pd.to_datetime(data['time'], unit='s')
data.set_index('time', inplace=True)
data.rename(columns={'open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'tick_volume':'Volume'}, inplace=True)
data = data[['Open', 'High', 'Low', 'Close', 'Volume']]

# -----------------------------
# Precompute rolling columns
# -----------------------------
data['leftHigh'] = data['High'].rolling(120).max()
data['cupLow'] = data['Low'].rolling(120).min()
data['handleHigh'] = data['High'].rolling(21).max()
data['handleLow'] = data['Low'].rolling(21).min()
data['avgVol'] = data['Volume'].rolling(21).mean()
data['fiftyTwoWeekHigh'] = data['High'].rolling(252).max()
data['priceChange'] = (data['Close'] - data['Close'].shift(252)) / data['Close'].shift(252)
data['ma200'] = data['Close'].rolling(200).mean()
data['ma50'] = data['Close'].rolling(50).mean()
data['baseHigh'] = data['High'].rolling(60).max()
data['baseLow'] = data['Low'].rolling(60).min()
data['avgVol50'] = data['Volume'].rolling(50).mean()

print(data.tail())

# -----------------------------
# More practical, flexible strategy
# -----------------------------
class OneilCANSLIMPractical(Strategy):
    def init(self):
        pass

    def next(self):
        i = -1

        close = self.data.Close[i]
        high = self.data.High[i]
        low = self.data.Low[i]
        volume = self.data.Volume[i]

        leftHigh = self.data.leftHigh[i]
        cupLow = self.data.cupLow[i]
        handleHigh = self.data.handleHigh[i]
        handleLow = self.data.handleLow[i]
        avgVol = self.data.avgVol[i]
        fiftyTwoWeekHigh = self.data.fiftyTwoWeekHigh[i]
        priceChange = self.data.priceChange[i]
        ma200 = self.data.ma200[i]
        ma50 = self.data.ma50[i]
        baseHigh = self.data.baseHigh[i]
        baseLow = self.data.baseLow[i]
        avgVol50 = self.data.avgVol50[i]

        # --- Looser C&H
        cupDepth = (leftHigh - cupLow) / leftHigh if leftHigh and cupLow else np.nan
        handleDepth = (handleHigh - handleLow) / handleHigh if handleHigh and handleLow else np.nan
        tightHandleRange = handleDepth <= 0.08 if not np.isnan(handleDepth) else False
        validHandle = handleDepth <= 0.10 if not np.isnan(handleDepth) else False
        volDecline = volume < avgVol if avgVol else False
        nearHigh = close >= 0.99 * fiftyTwoWeekHigh if fiftyTwoWeekHigh else False

        cupHandleSignal = (
            (cupDepth >= 0.15 if not np.isnan(cupDepth) else False)
            and (high >= leftHigh if leftHigh else False)
            and validHandle and volDecline and tightHandleRange and nearHigh
        )

        # --- Looser CANSLIM
        strongMomentum = priceChange >= 0.30 if not np.isnan(priceChange) else False
        aboveMA200 = close > ma200 if not np.isnan(ma200) else False
        aboveMA50 = close > ma50 if not np.isnan(ma50) else False

        canslimSignal = strongMomentum and aboveMA200 and aboveMA50 and nearHigh

        # --- Looser Base Breakout
        baseDepth = (baseHigh - baseLow) / baseHigh if baseHigh and baseLow else np.nan
        baseBreakout = (
            (high >= baseHigh if baseHigh else False)
            and (baseDepth <= 0.12 if not np.isnan(baseDepth) else False)
            and (volume > 2 * avgVol50 if avgVol50 else False)
            and nearHigh
        )

        signalScore = sum([cupHandleSignal, canslimSignal, baseBreakout])

        print(f"{self.data.index[-1].date()} | Close={close:.2f} | cupDepth={cupDepth:.2f} | handleDepth={handleDepth:.2f} | priceChange={priceChange:.2f} | nearHigh={nearHigh} | C&H={cupHandleSignal} | CANSLIM={canslimSignal} | BASE={baseBreakout} | Score={signalScore}")

        if signalScore > 0:
            if not self.position:
                self.buy()
        else:
            if self.position:
                self.position.close()

# -----------------------------
# Run backtest
# -----------------------------
bt = Backtest(data, OneilCANSLIMPractical, cash=10_000, commission=0.002)
results = bt.run()
print(results)
bt.plot()

# -----------------------------
# Shutdown connection
# -----------------------------
mt5.shutdown()
