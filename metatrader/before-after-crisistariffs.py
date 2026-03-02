import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()


symbols = ["#ETN-T","#CLS-T","#ULTA-T","#ANET-T","#DLTR-T","#BA-T","#WMT-T","#SMCI.US-T","#NVDA-T","#AMD-T","#WBD.US-T","#NKE-T","#FIX-T"]

LOGIN = ADMIRALS_ACCOUNT
PASSWORD = os.getenv('ADMIRAL_PASSWORD')
SERVER = "AdmiralsSC-Demo"
MT5_PATH = r"C:\\Program Files\\Admirals SC MT5 Terminal\\terminal64.exe"

if not mt5.initialize(path=MT5_PATH, login=LOGIN, password=PASSWORD, server=SERVER):
    print(f"Failed to connect: {mt5.last_error()}")
    quit()

for sym in symbols:
    mt5.symbol_select(sym, True)

from_date = datetime(2025, 4, 7)
to_date   = datetime(2025, 5, 16)

for sym in symbols:
    rates = mt5.copy_rates_range(sym, mt5.TIMEFRAME_D1, from_date, to_date)
    if rates is None or len(rates) == 0:
        print(f"No data for {sym}")
        continue
    df = pd.DataFrame(rates)
    open_price = df.iloc[0]['open']
    close_price = df.iloc[-1]['close']
    pct_change = ((close_price / open_price) - 1) * 100
    print(f"{sym}: {pct_change:.2f}%")

mt5.shutdown()
