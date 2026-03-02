import pandas as pd
import MetaTrader5 as mt5

import os
nombre = int(os.getenv("ROBOFOREX_LOGIN", "0"))
clave = os.getenv("ROBOFOREX_PASSWORD")
servidor = os.getenv("ROBOFOREX_SERVER", "RoboForex-ECN")
path = r'C:\Program Files\RoboForex MT5 Terminal\terminal64.exe' #la puedo reemplazar por el path donde está el terminal de MT5 de mi broker, 

mt5.initialize(login = nombre, password = clave, server = servidor, path = path)

rates = mt5.copy_rates_from_pos('EURUSD', mt5.TIMEFRAME_M1, 0, 9999)
tabla = pd.DataFrame(rates)
tabla['time'] = pd.to_datetime(tabla['time'], unit='s')

''''
# Si quiero ver los datos de otra acción, por ejemplo IBM, puedo hacer lo siguiente:
rates = mt5.copy_rates_from_pos('IBM', mt5.TIMEFRAME_M1, 0, 9999)
tabla = pd.DataFrame(rates)
tabla['time'] = pd.to_datetime(tabla['time'], unit='s')
'''

tabla.tail()

trade = {'action': mt5.TRADE_ACTION_DEAL, #es operación de mercado
         'type': mt5.ORDER_TYPE_BUY, #especifica que la orden es de compra (BUY).
         'symbol': 'XAUUSD',#ticker del oro y USD 
         'volume': 0.01, #no una onza de oro, sino 0.01 lotes, que es una fracción de un lote estándar. 0.1 también podría funcionar para una acción, si lo que quiero es acciones fraccionadas
         'comment': 'DT', # comentario de la orden, identificador que le doy, random
         'type_filling': mt5.ORDER_FILLING_FOK, # tipo de orden, IOC significa Immediate or Cancel, es decir, que si no se puede ejecutar inmediatamente, se cancela
         } 
mt5.order_send(trade) #sin esto, no ejecuta

for i in range(10): #hacer 10 veces la orden de compra o venta
    trade = {'action': mt5.TRADE_ACTION_DEAL, #es operación de mercado
         'type': mt5.ORDER_TYPE_SELL, #especifica que la orden es de compra (BUY).
         'symbol': 'XAUUSD',#ticker del oro y USD 
         'volume': 0.01, #no una onza de oro, sino 0.01 lotes, que es una fracción de un lote estándar. 0.1 también podría funcionar para una acción, si lo que quiero es acciones fraccionadas
         'comment': 'DT', # comentario de la orden, identificador que le doy, random
         'type_filling': mt5.ORDER_FILLING_FOK, # tipo de orden, IOC significa Immediate or Cancel, es decir, que si no se puede ejecutar inmediatamente, se cancela
         } 
    mt5.order_send(trade) 
    #envío la orden 10 veces, para ver si funciona

trade_pending1 = {'action': mt5.TRADE_ACTION_PENDING, #es operación pendiente
                  'type': mt5.ORDER_TYPE_BUY_STOP, #especifica que la orden es de compra (BUY).
                  'price': mt5.symbol_info_tick('EURUSD').ask + 0.0005, #precio al que quiero que se ejecute la orden pendiente, es decir, si el precio sube 10 puntos desde el precio actual, se ejecuta la orden. precio ask al que la gente está dispuesta a vender. precio bid, el máximo al que la gente está dispuesta a comprar. Más alto al que la gente está dispuesta a comprar, más alto al que el mercado tiene.
                  'type_filling': mt5.ORDER_FILLING_FOK,
                  'comment': 'DavidT', # comentario de la orden, identificador que le doy, rando
                  'volume': 0.5, 
                  'symbol': 'EURUSD', #ticker del euro y USD. Es obligatorio.
                  } #es operación pendiente
mt5.order_send(trade_pending1) 

trade_pending12 = {'action': mt5.TRADE_ACTION_PENDING, #es operación pendiente
                  'type': mt5.ORDER_TYPE_BUY_LIMIT, #especifica que la orden es de compra (BUY).
                  'price': mt5.symbol_info_tick('EURUSD').ask- 0.0005, #precio al que quiero que se ejecute la orden pendiente, es decir, si el precio sube 10 puntos desde el precio actual, se ejecuta la orden. precio ask al que la gente está dispuesta a vender. precio bid, el máximo al que la gente está dispuesta a comprar. Más alto al que la gente está dispuesta a comprar, más alto al que el mercado tiene.
                  'type_filling': mt5.ORDER_FILLING_FOK,
                  'comment': 'DavidT', # comentario de la orden, identificador que le doy, rando
                  'volume': 0.5, 
                  'symbol': 'EURUSD', #ticker del euro y USD. Es obligatorio.
                  } #es operación pendiente
mt5.order_send(trade_pending12) 

closerorder = {'action': mt5.TRADE_ACTION_DEAL, #es operación de mercado
                  'type': mt5.ORDER_TYPE_SELL, #especifica que la orden es de venta
                  'position':512664996, #número o código de la orden que quiero cerrar, es decir, el número de la orden pendiente que quiero cerrar. Puedo ver el número de la orden pendiente en la pestaña "Operaciones" del terminal de MT5.
                  'symbol': 'EURUSD', #ticker del euro y USD. Es obligatorio.   
                  'volume': 0.01, #no una onza de oro, sino 0.01 lotes, que es una fracción de un lote estándar. 0.1 también podría funcionar para una acción, si lo que quiero es acciones fraccionadas
                  'type_filling': mt5.ORDER_FILLING_FOK,}
mt5.order_send(closerorder) 

ops_abiertas = mt5.positions_get()
df_positions = pd.DataFrame(list(ops_abiertas ), columns = ops_abiertas)

list_tickets = df_positions['ticket'].unique().tolist()

for ticket in list_tickets:
    print(ticket)
    df_trade = df_positions[df_positions['ticket']] ==#



    

