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

