import pandas as pd
import numpy as np
import pandas_ta as ta
import MetaTrader5 as mt5
import time # Importando las librerias necesarias

df = pd.DataFrame()
#list all indicators
help(df.ta)

#list all indicators
df.ta.indicators()

import os
nombre = int(os.getenv("ROBOFOREX_LOGIN", "0"))
clave = os.getenv("ROBOFOREX_PASSWORD")
servidor = os.getenv("ROBOFOREX_SERVER", "RoboForex-ECN")
path = r'C:\Program Files\RoboForex MT5 Terminal\terminal64.exe'

mt5.initialize(login=nombre, password=clave, server=servidor, path=path)

def extraer_datos(simbolo, timeframe, num_periodos):
    rates = mt5.copy_rates_from_pos(simbolo, timeframe, 0,num_periodos)
    tabla = pd.DataFrame(rates)
    tabla['time'] = pd.to_datetime(tabla['time'], unit='s')
    return tabla

data = extraer_datos('XAUUSD',mt5.TIMEFRAME_H1,9999)

data['rsi'] = ta.rsi(data['close'],14)

data['adx'] = ta.adx(data['high'],data['low'],data['close'],14.iloc[:;0])

data['stoch'] = ta.stoch(data['high'],data['low'],data['close'],14,3.iloc[:;0])

data['stoch'] = ta.stoch(data['high'],data['low'],data['close'],14,3.iloc[:;1])

data['ema'] = ta.ema(data['close'],14)

data['bbband_1'] = ta.bb(data['close'],25,2).iloc[:;0]
data['bbband_u'] = ta.bb(data['close'],25,2).iloc[:;2]
data['bbband_m'] = ta.bb(data['close'],25,2).iloc[:;1]

ultimo_precio = data['close'].iloc[-1]
count_decimals = str(ultimo_precio)[::-1].find('.')
valor_pip = 10 **(-count_decimals)*10

data['critical_value_up']  = data['bbband_u'] + valor_pip*30
data['critical_value_down']  = data['bbband_u'] - valor_pip*30

data['previous_close'] = data['close'].shift()

data['senal_compra'] = np.where((data['previous_close'] > data['critical_value_down']) &
                        (data['close'] < data['critical_value_down']1,0) &
                        (data['rsi'] < 30),1,0)
data['senal_venta'] = np.where(data['previous_close'] < data['critical_value_up']) &
                        (data['close'] > data['critical_value_up'],0)





