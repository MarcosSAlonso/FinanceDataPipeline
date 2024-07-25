# -*- coding: utf-8 -*-
import data_processing as dp
import database as db

# Configurar tickers y fechas
tickers = ['GOOG','TSM', 'NVDA', 'AMZN', 'META']
new_ticker = 'AAPL'
start_date = "2020-01-01"
end_date = "2024-07-23"

# Descargar datos para tickers existentes
data = dp.download_data(tickers, start_date, end_date)

# Procesar y almacenar datos en la base de datos
db.store_data_in_db(data)

# Agregar datos para la nueva empresa
new_data = dp.download_data([new_ticker], start_date, end_date)
db.store_data_in_db(new_data)


