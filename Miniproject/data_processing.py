# -*- coding: utf-8 -*-
import yfinance as yf

# Función para descargar datos de Yahoo Finance
def download_data(tickers, start_date, end_date):
    data_to_insert = []
    for ticker in tickers:
        data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)
        if data.empty:
            print(f"Advertencia: No se encontraron datos para el ticker {ticker}")
            continue
        
        for index, row in data.iterrows():
            if all(col in row.index for col in ['Open', 'High', 'Low', 'Close', 'Volume']):
                adj_close = row['Adj Close'] if 'Adj Close' in row.index else 0
                data_to_insert.append((index.date(), ticker, row['Open'], row['High'], row['Low'], row['Close'], adj_close, row['Volume']))
            else:
                print(f"Advertencia: Faltan columnas en los datos para el ticker {ticker} en la fecha {index}")
                
    return data_to_insert


