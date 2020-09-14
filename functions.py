
"""
# -- --------------------------------------------------------------------------------------------------- -- #
# -- project: A SHORT DESCRIPTION OF THE PROJECT                                                         -- #
# -- script: functions.py : python script with general functions                                         -- #
# -- author: YOUR GITHUB USER NAME                                                                       -- #
# -- license: GPL-3.0 License                                                                            -- #
# -- repository: YOUR REPOSITORY URL                                                                     -- #
# -- --------------------------------------------------------------------------------------------------- -- #
"""
import pandas as pd
import numpy as np
import time as time
import yfinance as yf
import math
#%% Paso 1.3
# Construir el vector de fechas a partir del vector de nombres de archivos

# estas serviran como etiquetas en dataframe y para yfinance

def f_fechas(p_archivos):

    t_fechas = [j.strftime('%Y-%m-%d') for j in sorted([pd.to_datetime(i[8:]).date() for i in p_archivos])]
    # lista con fechas ordenadas (para usarse como indexadores de archivos)
    i_fechas = [j.strftime('%d%m%y') for j in sorted([pd.to_datetime(i[8:]).date() for i in p_archivos])]
    r_f_fechas = {'i_fechas': i_fechas, 't_fechas': t_fechas}


    return r_f_fechas

#%% Paso 1.4

# Construir el vector de tickers utilizables en yahoo finance
def f_global_tickers(data_archivos, filenames):
    tickers = []

    #l_ticker = [[tickers.append(j + 'MX') for j in [data_archivos[i]['Ticker'] for i in filenames]]

    [tickers.append(j +'.MX') for j in [data_archivos[i]['Ticker'] for i in filenames]]
    # ticker se queda como una sola lista
    tickers = np.concatenate(tickers)
    global_tickers = np.unique(tickers).tolist()

    return global_tickers

#%% Paso 1.5
# Descargar y acomodar el todos los precios históricos

def prices_download(global_tickers):
    global_tickers = [i.replace('GFREGIOO.MX', 'RA.MX') for i in global_tickers]
    global_tickers = [i.replace('MEXCHEM.MX', 'ORBIA.MX') for i in global_tickers]
    global_tickers = [i.replace('LIVEPOLC.1.MX', 'LIVEPOLC-1.MX') for i in global_tickers]

    # eliminamos MXN, USD, KOFL

    [global_tickers.remove(i) for i in ['MXN.MX', 'USD.MX', 'KOFL.MX', 'KOFUBL.MX', 'BSMXB.MX']]

    inicio = time.time()
    data = yf.download(global_tickers, start="2017-08-21", end="2020-08-22", actions=False, group_by="close", interval="1d",
                       auto_adjust=False, prepost=False, threads=True)
    print('se tardo', time.time() - inicio, 'segundos')


    return data

#%% Paso 1.7
# Limpiamos y ordenamos los precios de data que nos interesan
def clean_price(data_archivos,dates_list,data, t_fechas):
    ini_tickers = []
    [ini_tickers.append(j + '.MX') for j in [data_archivos[dates_list[0]]['Ticker']]]
    ini_tickers = np.concatenate(ini_tickers)
    # Obtener posisiones históricas de yahoo

    ini_tickers = [i.replace('GFREGIOO.MX', 'RA.MX') for i in ini_tickers]
    ini_tickers = [i.replace('MEXCHEM.MX', 'ORBIA.MX') for i in ini_tickers]
    ini_tickers = [i.replace('LIVEPOLC.1.MX', 'LIVEPOLC-1.MX') for i in ini_tickers]


    # quitamos los tickers que no están en el archivo inicial
    [ini_tickers.remove(i) for i in ['MXN.MX', 'KOFL.MX', 'BSMXB.MX']]


    # Obtenemos los precios de cierre solamente
    data_close = pd.DataFrame({i: data[i]['Close'] for i in ini_tickers})
    # Localizamos los precios en las fechas de interés
    precios = data_close.iloc[np.concatenate([np.where(data_close.index.astype(str) == i)[0] for i in t_fechas])]
    # ordenar cloumnas lexicograficamente
    precios = precios.reindex(sorted(precios.columns), axis=1)

    return precios, data_close
#%% Paso 1.8
# comenzamos a estructurar la inversión pasiva
def pasiva_ini(data_archivos, dates_list, precios):
    # Posición inicial
    # capital inicial
    k = 1000000
    # comisiones por transaccion
    c = 0.00125

    c_activos = ['KOFL', 'KOFUBL', 'BSMXB', 'MXN', 'USD']
    # dinero asignado a cada rubro
    # money  = k*np.array(data_archivos[dates_list[0]]['Peso (%)'])

    # redondeamos hacia abajo el número de acciones a comprar
    # amount_stocks = [math.floor(j) for j in [money[i]/data_archivos[dates_list[0]]['Precio'][i] for i in range(35)]]

    inv_pasiva = {'timestamp': ['05-01-2018'], 'capital': [k]}

    pos_datos = data_archivos[dates_list[0]].copy().sort_values('Ticker')[['Ticker', 'Nombre', 'Peso (%)']]

    # extraer la lista de los archivos a eliminar
    i_activos = list(pos_datos[list(pos_datos['Ticker'].isin(c_activos))].index)

    # eliminar los activos del archivo
    pos_datos.drop(i_activos, inplace=True)

    # reseteamos el index
    pos_datos.reset_index(inplace=True, drop=True)

    # agreagamos .MX para empatar precios
    pos_datos['Ticker'] = pos_datos['Ticker'] + '.MX'

    # corregir tickers en datos
    pos_datos['Ticker'] = pos_datos['Ticker'].replace('LIVEPOLC.1.MX', 'LIVEPOLC-1.MX')
    pos_datos['Ticker'] = pos_datos['Ticker'].replace('MEXCHEM.MX', 'ORBIA.MX')
    pos_datos['Ticker'] = pos_datos['Ticker'].replace('GFREGIOO.MX', 'RA.MX')

    match = 0
    #precios.index.to_list()[match]
    m2 = [precios.iloc[match, precios.columns.to_list().index(i)] for i in pos_datos['Ticker']]
    pos_datos['Precio'] = m2
    return pos_datos

#%% Paso 1.9
# realizamos todos los pasos de la inversión pasiva
def inv_pasiva(pos_datos, dates_list, precios):
    k = 1000000
    c = 0.00125

    # capital destinado por acción = proporción capital - comisiones por la posición
    pos_datos['Capital'] = pos_datos['Peso (%)'] * k - pos_datos['Peso (%)'] * k * c

    # cantidad de títulos por acción
    pos_datos['Títulos'] = pos_datos['Capital'] / pos_datos['Precio']

    # redondeamos hacia abajo los títulos para no comprar fracciones de acción
    pos_datos['Títulos'] = [math.floor(pos_datos['Títulos'][i]) for i in range(pos_datos.shape[0])]
    # multiplicar el precio por la cantidad de títulos
    pos_datos['Postura'] = pos_datos['Títulos'] * pos_datos['Precio']

    # calcular la comisión por pagar la postura
    pos_datos['Comisión'] = pos_datos['Títulos'] * pos_datos['Precio'] * c

    # calculamos la suma de todas posiciones
    pos_value = pos_datos['Postura'].sum()

    # Ahora calculamos cuanto dinero se quedo en cash
    cash = 1000000 - pos_value - pos_datos['Comisión'].sum()
    Capital = pos_value + cash
    #inv_pasiva = collections.defaultdict(list)
    inv_pasiva = [{'timestamp': '30-08-2018', 'Capital': 1000000, 'rend': 0, 'rend_acum': 0}]
    #  Empezamos el ciclo para correr todo

    for i in range(0, len(dates_list)):
        match = i
        #precios.index.to_list()[match]
        m2 = [precios.iloc[match, precios.columns.to_list().index(i)] for i in pos_datos['Ticker']]
        pos_datos['Precio'] = m2
        # multiplicar el precio por la cantidad de títulos
        pos_datos['Postura'] = pos_datos['Títulos'] * pos_datos['Precio']

        # calculamos la suma de todas posiciones
        pos_value = pos_datos['Postura'].sum()
        # calculamos el capital
        Capital = pos_value + cash
        rend = (Capital - inv_pasiva[i]['Capital']) / inv_pasiva[i]['Capital']
        acum = rend + inv_pasiva[i]['rend_acum']
        inv_pasiva.append({'timestamp': dates_list[i], 'Capital': Capital, 'rend': rend, 'rend_acum': acum})

    inv_pasiva = pd.DataFrame(inv_pasiva)

    return inv_pasiva

#%% paso 1.10
# se inicializa la parte de inversión activa
# tomamos las misma ponderaciones iniciales del archivo del 31 de enero 2018 y hacemos sort con los pesos de mayor a menor
def activa_ini(data_archivos, dates_list, precios):
    activa_ini =  data_archivos[dates_list[0]].copy().sort_values('Peso (%)', ascending=False )[['Ticker', 'Nombre', 'Peso (%)']]
    # extraer la lista de los archivos a eliminar

    c_activos = ['KOFL', 'KOFUBL', 'BSMXB', 'MXN', 'USD']
    i_activos = list(activa_ini[list(activa_ini['Ticker'].isin(c_activos))].index)

    # eliminar los activos de activa_ini
    activa_ini.drop(i_activos, inplace=True)

    #reseteamos el index
    activa_ini.reset_index(inplace=True, drop=True)

    #agreagamos .MX para empatar precios
    activa_ini['Ticker'] = activa_ini['Ticker'] + '.MX'

    #corregir tickers en datos
    activa_ini['Ticker'] = activa_ini['Ticker'].replace('LIVEPOLC.1.MX', 'LIVEPOLC-1.MX')
    activa_ini['Ticker'] = activa_ini['Ticker'].replace('MEXCHEM.MX', 'ORBIA.MX')
    activa_ini['Ticker'] = activa_ini['Ticker'].replace('GFREGIOO.MX', 'RA.MX')

    #como inicializaremos el portafolio con la mitad del capital para el mayor activo, reducimos a la mitad el peso de AMXL
    activa_ini = activa_ini.copy()
    activa_ini.loc[0, 'Peso (%)']= activa_ini.loc[0, 'Peso (%)']/2

    # Posición inicial
    # capital inicial

    k = 1000000
    # comisiones por transaccion
    c = 0.00125

    match = 0
    #precios.index.to_list()[match]
    m2 = [precios.iloc[match, precios.columns.to_list().index(i)] for i in activa_ini['Ticker']]
    activa_ini['Precio'] = m2

    #capital destinado por acción = proporción capital - comisiones por la posición
    activa_ini['Capital'] = activa_ini['Peso (%)']*k - activa_ini['Peso (%)']*k*c

    #cantidad de títulos por acción
    activa_ini['Títulos'] = activa_ini['Capital']/activa_ini['Precio']

    # redondeamos hacia abajo los títulos para no comprar fracciones de acción
    activa_ini['Títulos'] = [math.floor(activa_ini['Títulos'][i]) for i in range(activa_ini.shape[0])]

    #multiplicar el precio por la cantidad de títulos
    activa_ini['Postura'] = activa_ini['Títulos']*activa_ini['Precio']

    #calcular la comisión por pagar la postura
    activa_ini['Comisión'] = activa_ini['Títulos']*activa_ini['Precio']*c

    #calculamos la suma de todas posiciones
    value_activa = activa_ini['Postura'].sum()

    #Ahora calculamos cuanto dinero se quedo en cash
    cash_ini = 1000000-value_activa - activa_ini['Comisión'].sum()
    cash_activa = 1000000-value_activa - activa_ini['Comisión'].sum()

    return activa_ini, cash_ini, cash_activa
#%% paso 1.11
# Empezamos la parte del rebalanceo
def operaciones(activa_ini, data, data_close,t_fechas,cash_activa):
    Amxl = data['AMXL.MX'][['Open', 'Close']]
    # tomamos el índice de interés
    indx = np.concatenate(np.where(data_close.index.astype(str) == t_fechas[0]))[0]
    # tomamos los precios de Amxl solo desde la fecha
    Amxl = Amxl.iloc[indx:, ]

    # comenzamos a diseñar la señal de compra
    X = -0.01
    kc = 0.1
    c = 0.00125
    historical = [{'timestamp': '30-01-2018', 'titulos_totales': activa_ini['Títulos'][0],
                   'titulos_compra': activa_ini['Títulos'][0], 'precio': activa_ini['Precio'][0],
                   'comisión': activa_ini['Comisión'][0], 'comisión_acum': activa_ini['Comisión'][0]}]

    saved_cash = [{}]
    for i in range(len(Amxl) - 1):  # quitamos uno para que no interfiera con el último dato
        if Amxl['Close'][i] / Amxl['Open'][i] - 1 <= X:
            available = cash_activa * kc - cash_activa * kc * c  # restamos la parte de las comisiones
            new_titulos = math.floor(available / Amxl['Open'][i + 1])
            if new_titulos >= 1:  # para ver si al menos todavía podemos comprar una accion
                postura = new_titulos * Amxl['Open'][i + 1]
                fecha = Amxl['Open'].index[i + 1].strftime('%d-%m-%Y')
                precio = Amxl['Open'][i + 1]
                comision = round(postura * c, 2)
                titulos_totales = historical[-1]['titulos_totales'] + new_titulos
                comision_acum = historical[-1]['comisión_acum'] + comision
                historical.append({'timestamp': fecha, 'titulos_totales': titulos_totales,
                                   'titulos_compra': new_titulos, 'precio': precio, 'comisión': comision,
                                   'comisión_acum': comision_acum})
                cash_activa = round(cash_activa - postura - comision, 2)
                saved_cash.append({'timestmap': fecha, 'cash': cash_activa})
            else:
                break


    historical = pd.DataFrame(historical)
    saved_cash = pd.DataFrame(saved_cash).dropna()
    return historical, saved_cash

#%% paso 1.12
# obtenemos de manera mensual, la actividad de la inversión activa


# hay que hacer una lista de listas con los días para cada mes del año desde que iniciamos el portafolio
def inv_activa(historical,activa_ini,dates_list,precios,saved_cash,cash_ini):

    months = [pd.date_range(dates_list[i], dates_list[i + 1]) for i in range(len(dates_list) - 1)]

    meses_str = [[months[i][j].strftime('%d-%m-%Y') for j in range(len(months[i]))] for i in range(len(months))]

    # Queremos ver la última vez que hubo operación por mes y reemplazar ese número en df mensual
    new_titulos = []
    for i in range(len(meses_str)):
        indx = np.where(historical['timestamp'].isin(meses_str[i]))[0]
        new_titulos.append(indx)

    last_cash = [saved_cash['cash'].iloc[x[-1] - 1] for x in new_titulos if x.size > 0]
    # limpiamos los arrays que quedaron vacios y seleccionamos los últimos titulos registrados
    new_titulos = [historical['titulos_totales'].iloc[x[-1]] for x in new_titulos if x.size > 0]


    # En activa_ini hacemos la multiplicacion de precios * Titulos, pero reemplazamos todos los titulos de AMXL
    inv_activa = [{'timestamp': '30-01-2018', 'Capital': 1000000, 'rend': 0, 'rend_acum': 0}]

    for i in range(0, len(dates_list)):
        match = i
        #precios.index.to_list()[match]
        m2 = [precios.iloc[match, precios.columns.to_list().index(i)] for i in activa_ini['Ticker']]
        activa_ini['Precio'] = m2
        # multiplicar el precio por la cantidad de títulos

        # aquí hacemos los cambios de las veces que hicimos operación
        if i == 0:  # la primera vez es un caso especial

            activa_ini['Postura'] = activa_ini['Títulos'] * activa_ini['Precio']
            # calculamos la suma de todas posiciones
            pos_value = activa_ini['Postura'].sum()
            # calculamos el capital
            Capital = pos_value + cash_ini  # este es el cash antes de que inicie todo
            rend = (Capital - inv_activa[i]['Capital']) / inv_activa[i]['Capital']
            acum = rend + inv_activa[i]['rend_acum']
            inv_activa.append({'timestamp': dates_list[i], 'Capital': Capital, 'rend': rend, 'rend_acum': acum})


        elif i <= len(new_titulos) & i >= 1:

            activa_ini = activa_ini.copy()
            activa_ini.loc[0, 'Títulos'] = new_titulos[i - 1]
            activa_ini['Postura'] = activa_ini['Títulos'] * activa_ini['Precio']
            # calculamos la suma de todas posiciones
            pos_value = activa_ini['Postura'].sum()
            # calculamos el capital
            Capital = pos_value + last_cash[i - 1]  # aquí añadimos el cash que nos queda al final del mes
            rend = (Capital - inv_activa[i]['Capital']) / inv_activa[i]['Capital']
            acum = rend + inv_activa[i]['rend_acum']
            inv_activa.append({'timestamp': dates_list[i], 'Capital': Capital, 'rend': rend, 'rend_acum': acum})

        else:
            activa_ini['Postura'] = activa_ini['Títulos'] * activa_ini['Precio']
            # calculamos la suma de todas posiciones
            pos_value = activa_ini['Postura'].sum()
            # calculamos el capital
            Capital = pos_value  # aquí ya no ponemos más cash porque ya se termino
            rend = (Capital - inv_activa[i]['Capital']) / inv_activa[i]['Capital']
            acum = rend + inv_activa[i]['rend_acum']
            inv_activa.append({'timestamp': dates_list[i], 'Capital': Capital, 'rend': rend, 'rend_acum': acum})


    inv_activa = pd.DataFrame(inv_activa)

    return inv_activa
