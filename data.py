# -*- coding: utf-8 -*-
"""
Editor de Spyder

Este es un archivo temporal.
"""
import time as time
import pandas as pd
import numpy as np
from datetime import datetime
from glob import glob
import yfinance as yf
import collections
import math


filenames = glob(r"C:\Users\manue\Documents\Documentos\Microestructura y sistemas de trading\myst_if706882_lab1\files\NAFTRAC_holdings\*.csv")
# data = pd.read_csv(filenames, skiprows=2)
archivos = [filenames[i][-18:-4] for i in range(len(filenames))]

# date =datetime.strptime(real, '%d%m%y').date()
# Leer todos los archivos y guardarlos en un diccionario
# crear un diccionario para almacenar todos los datos
data_archivos = {}

for i in filenames:
    data = pd.read_csv(i, skiprows=2, header=None)
    # renombrar las columnas con lo que tiene el 1er renglon
    data.columns = list(data.iloc[0, :])
    # quitar las columnas que no sean nan
    data = data.loc[:, pd.notnull(data.columns)]
    # reiniciar índice
    data = data.iloc[1:-1].reset_index(drop=True, inplace=False)
    # quitar las comas de la columna Precio
    data['Precio'] = [i.replace(',', '') for i in data['Precio']]
    # quitar asterisco de la colmuna ticker
    data['Ticker'] = [i.replace('*', '') for i in data['Ticker']]
    # hacer conversiones de tipos de columna a númerico
    convert_dict = {'Ticker': str, 'Nombre': str, 'Peso (%)': float, 'Precio': float}
    data = data.astype(convert_dict)
    # convertir a decimal la columna de peso (%)
    data['Peso (%)'] = data['Peso (%)'] / 100
    data_archivos[i] = data


