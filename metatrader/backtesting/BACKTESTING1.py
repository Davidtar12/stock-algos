import MetaTrader5 as mt5
import pandas as pd
from backtesting import Backtest, Strategy
from Easy_Trading import Basic_funcs
import pandas_ta as ta
from datetime import datetime
from random import random

import os
nombre = int(os.getenv("ADMIRALS_LOGIN", "0"))
clave = os.getenv("ADMIRALS_PASSWORD")
servidor = os.getenv("ADMIRALS_SERVER", "AdmiralsSC-Demo")
path = r"C:\\Program Files\\Admirals SC MT5 Terminal\\terminal64.exe"

bfs = Basic_funcs(nombre,clave,servidor,path)

class Estrategia_simple(Strategy):
    def init(self):
        self.price_close = self.data.Close
        self.price_open = self.data.Open

    def next(self):
        if self.price_close[-1] > self.data.Open:
            self.position.close()  # Close any existing positio
            self.buy()
        elif self.price_close[-1] < self.data.Open:
            self.sell() 

datos = bfs._get_data_for_bt(mt5.TIMEFRAME_D1, "EURUSD-T",200)

backtesting1 = Backtest(datos, Estrategia_simple, cash=10000, commission=.002, exclusive_orders=True)
stats1 = backtesting1.run()
backtesting1.plot()
stats1._trades
#comparar con random?
random.seed(123)

def simulate_coin_flip():
    if random.random() <= 0.5:
        result_final = 'Cara'
    else:
        result_final = 'Sello'
    return result_final

class Estrategia_aleatoria(Strategy):
    def init(self):
        self.price_close = self.data.Close
        self.price_open = self.data.Open

    def next(self):
        resultado = simulate_coin_flip()
        print(resultado)

        if resultado == 'Cara':
            self.position.close()  # Close any existing position
            self.sell()
        else:
            self.position.close()
            self.buy()
