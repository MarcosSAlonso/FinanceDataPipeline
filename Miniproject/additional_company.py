# -*- coding: utf-8 -*-
import data_processing as dp
import database as db

# Configurar el nuevo ticker y fechas
new_ticker = 'VZ'
start_date = "2020-01-01"
end_date = "2024-07-23"

# Descargar datos para la nueva empresa
data = dp.download_data([new_ticker], start_date, end_date)

# Procesar y almacenar datos en la base de datos
db.store_data_in_db(data)

