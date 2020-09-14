
"""
# -- --------------------------------------------------------------------------------------------------- -- #
# -- project: A SHORT DESCRIPTION OF THE PROJECT                                                         -- #
# -- script: main.py : python script with the main functionality                                         -- #
# -- author: YOUR GITHUB USER NAME                                                                       -- #
# -- license: GPL-3.0 License                                                                            -- #
# -- repository: YOUR REPOSITORY URL                                                                     -- #
# -- --------------------------------------------------------------------------------------------------- -- #
"""

import data as dt
import functions as fn
import datetime as datetime
# -- Obtener la lista de los archivos a leer

# data.py
# Paso 1.1
# -- Obtener la lista de los archivos a leer

# data.py
# Paso 1.2
# -- Leer todos los archivos y guardarlos en un diccionario

# functions.py
# Paso 1.3
# construir
fechas = fn.f_fechas(p_archivos=dt.archivos)

# functions.py
# paso 1.4
# Construir el vector de tickers utilizables en yahoo finance

global_tickers = fn.f_global_tickers(data_archivos=dt.data_archivos, filenames=dt.filenames)

# functions.py
# paso 1.5
# Descargar y acomodar el todos los precios históricos
data = fn.prices_download(global_tickers=global_tickers)

# main.py
# obtener una lista y al mismo tiempo poner en formato fecha las llaves de data_archivos
# paso 1.6

dates_list = [datetime.datetime.strptime(i, "%d%m%y").date() for i in fechas['i_fechas']]
for i in range(len(fechas['i_fechas'])):
    for j in range(32):
        if fechas['i_fechas'][i] in dt.filenames[j]:
            dt.data_archivos[fechas['i_fechas'][i]]=dt.data_archivos.pop(dt.filenames[j])
            dt.data_archivos[dates_list[i]]=dt.data_archivos.pop(fechas['i_fechas'][i])

# functions.py
# paso 1.7
# preparar la tabla que usaremos para ir cotejando los precios de todos los meses
clean_prices, data_close = fn.clean_price(data_archivos=dt.data_archivos, dates_list=dates_list, data=data, t_fechas=fechas['t_fechas'])


# functions.py
# paso 1.8
# se inicializa la inversión pasiva
pos_datos = fn.pasiva_ini(data_archivos=dt.data_archivos, dates_list=dates_list, precios=clean_prices)

# functions.py
# paso 1.9
# obtenemos el resultado de la inversión pasiva
inv_pasiva = fn.inv_pasiva(pos_datos=pos_datos, dates_list=dates_list, precios=clean_prices)

# functions.py
# paso 1.10
# se inicializa la inversión activa

activa_ini,cash_activa,cash_ini = fn.activa_ini(data_archivos=dt.data_archivos, dates_list=dates_list,precios=clean_prices)

# functions.py
# paso 1.11
# se obtiene el historico de operaciones
historical, saved_cash = fn.operaciones(activa_ini=activa_ini,
                                        data=data,data_close=data_close,t_fechas=fechas['t_fechas'],cash_activa=cash_activa)
# functions.py
# paso 1.12
# resultado final de llevar a cabo la inversión activa
inv_activa = fn.inv_activa(historical=historical,activa_ini=activa_ini,dates_list=dates_list,precios=clean_prices
                           ,saved_cash=saved_cash,cash_ini=cash_ini)