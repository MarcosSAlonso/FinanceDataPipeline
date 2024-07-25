# -*- coding: utf-8 -*-
import psycopg2
from psycopg2 import sql

# Función para conectar a la base de datos PostgreSQL
def connect_to_db():
    conn = psycopg2.connect(
        database="StockData",
        user="postgres",
        password="Ma260901",
        host="localhost",
        port=5432
    )
    return conn

# Función para crear la tabla si no existe
def create_table_if_not_exists():
    conn = connect_to_db()
    cur = conn.cursor()

    try:
        table_name = 'historical_data'

        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                Date DATE NOT NULL,
                Ticker VARCHAR(10) NOT NULL,
                Open NUMERIC,
                High NUMERIC,
                Low NUMERIC,
                Close NUMERIC,
                Adj_Close NUMERIC,
                Volume BIGINT,
                CONSTRAINT pk_date_ticker PRIMARY KEY (Date, Ticker)
            )
        """).format(sql.Identifier(table_name))
        cur.execute(create_table_query)
        conn.commit()
        print(f"Tabla '{table_name}' creada correctamente si no existía.")

    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()  # Revertir la transacción en caso de error
        print(f"Error al crear la tabla '{table_name}': {error}")

    finally:
        # Cerrar el cursor y la conexión
        if cur:
            cur.close()
        if conn:
            conn.close()

# Función para almacenar datos en la base de datos
def store_data_in_db(data):
    conn = connect_to_db()
    cur = conn.cursor()

    try:
        table_name = 'historical_data'
        # Preparar y ejecutar inserción de datos usando executemany
        insert_query = sql.SQL("""
            INSERT INTO {} (Date, Ticker, Open, High, Low, Close, Adj_Close, Volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """).format(sql.Identifier(table_name))
        cur.executemany(insert_query, data)

        # Confirmar los cambios en la base de datos
        conn.commit()
        print("Datos almacenados en PostgreSQL correctamente.")
        
    #Manejo de excepciones recibidas a la hora de probar el proyecto
    except psycopg2.IntegrityError as integrity_err:
        conn.rollback()  # Revertir la transacción en caso de error de integridad
        print(f"Error de integridad al insertar datos: {integrity_err}")

    except KeyError as key_err:
        conn.rollback()  # Revertir la transacción en caso de KeyError
        print(f"Error de clave al insertar datos: {key_err}")

    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()  # Revertir la transacción en caso de error general
        print(f"Error: {error}")

    finally:
        # Cerrar el cursor y la conexión
        if cur:
            cur.close()
        if conn:
            conn.close()

