import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import pandas_ta as ta
from dotenv import load_dotenv
import os
load_dotenv()


# === ACCOUNT LOGIN ===
login = int(os.getenv("ROBOFOREX_LOGIN", "0"))
password = os.getenv('SECONDARY_PASSWORD')
server = 'RoboForex-ECN'
terminal_path = r'C:\Program Files\RoboForex MT5 Terminal\terminal64.exe'

# === STRATEGY SETTINGS ===
symbol = '.US500Cash'  # Replace with your desired index or symbol
lot_size = 0.1

lengthHMA = 200
maxPctAboveHMA = 0.0080
minADX = 15
atrLength = 14
stopLossPct = 0.019
delayedStopLossPct = 0.01
delayedStopBars = 100
takeProfitPct = 0.006
atrTPmult = 1.5

marketOpen = 9 * 3600 + 30 * 60
exitBuffer = 15 * 3600 + 25 * 60
marketClose = 15 * 3600 + 30 * 60

# === MT5 Initialization ===
def initialize_mt5():
    if not mt5.initialize(login=login, password=password, server=server, path=terminal_path):
        raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")

# === ORDER FUNCTION ===
def send_order(order_type, volume):
    price = mt5.symbol_info_tick(symbol).ask if order_type == mt5.ORDER_TYPE_BUY else mt5.symbol_info_tick(symbol).bid
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "deviation": 10,
        "magic": 123456,
        "comment": "PythonTrade"
    }
    result = mt5.order_send(request)
    print(f"Order sent: {result.comment}")
    return result

# === Strategy Loop ===
def run_strategy():
    entry_price = None
    entry_bar = None
    in_trade = False

    while True:
        now = datetime.now()
        secs = now.hour * 3600 + now.minute * 60 + now.second
        bars = mt5.copy_rates_from(symbol, mt5.TIMEFRAME_M1, now - timedelta(minutes=300), 300)
        df = pd.DataFrame(bars)
        df['time'] = pd.to_datetime(df['time'], unit='s')

        # Calculate technical indicators using pandas_ta
        df['hma'] = ta.hma(df['close'], length=lengthHMA)
        df['adx'] = ta.adx(df['high'], df['low'], df['close'], length=atrLength)['ADX_14']
        df['obv'] = ta.obv(df['close'], df['tick_volume'])
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=atrLength)

        # Derived columns
        df['pct_above_hma'] = (df['close'] - df['hma']) / df['hma']
        df['obvSpike'] = df['obv'] > df['obv'].shift(1).rolling(10).max()
        df.dropna(inplace=True)

        if df.empty:
            time.sleep(60)
            continue

        latest = df.iloc[-1]
        bar_num = len(df)

        # === ENTRY CONDITION ===
        if not in_trade and marketOpen <= secs < marketClose:
            if (
                latest['close'] > latest['hma'] and
                latest['pct_above_hma'] <= maxPctAboveHMA and
                latest['adx'] > minADX and
                latest['obvSpike']
            ):
                send_order(mt5.ORDER_TYPE_BUY, lot_size)
                entry_price = latest['close']
                entry_bar = bar_num
                in_trade = True
                print(f"Entered at {entry_price} on {latest['time']}")

        # === EXIT CONDITIONS ===
        if in_trade:
            stop_loss = entry_price * (1 - stopLossPct)
            delayed_stop = entry_price * (1 - delayedStopLossPct)
            tp_fixed = entry_price * (1 + takeProfitPct)
            tp_atr = entry_price + atrTPmult * latest['atr']
            tp = min(tp_fixed, tp_atr)

            exit_reason = None
            if latest['close'] <= stop_loss:
                exit_reason = "Stop Loss"
            elif bar_num >= entry_bar + delayedStopBars and latest['close'] <= delayed_stop:
                exit_reason = "Delayed Stop"
            elif latest['close'] >= tp:
                exit_reason = "Take Profit"
            elif secs >= exitBuffer:
                exit_reason = "Time Exit"

            if exit_reason:
                send_order(mt5.ORDER_TYPE_SELL, lot_size)
                print(f"Exited at {latest['close']} due to {exit_reason}")
                in_trade = False

        time.sleep(60 - datetime.now().second)

# === RUN ===
if __name__ == '__main__':
    initialize_mt5()
    run_strategy()
    mt5.shutdown()
