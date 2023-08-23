import psycopg2
from psycopg2 import extras
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Parámetros de la base de datos
db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Consulta para obtener transacciones cruzadas entre CLAP y BANSUR
query_crossed_transactions = """
    SELECT
        COUNT(*) AS crossed_count
    FROM clap c
    JOIN bansur b ON c.INICIO6_TARJETA || c.FINAL4_TARJETA = b.TARJETA
                 AND c.MONTO = b.MONTO
                 AND c.FECHA_TRANSACCION = b.FECHA_TRANSACCION
"""

with psycopg2.connect(**db_params) as conn:
    with conn.cursor() as cursor:
        cursor.execute(query_crossed_transactions)
        crossed_count = cursor.fetchone()[0]

query_total_clap_transactions = """
    SELECT COUNT(*) FROM clap
"""

with psycopg2.connect(**db_params) as conn:
    with conn.cursor() as cursor:
        cursor.execute(query_total_clap_transactions)
        total_clap_transactions = cursor.fetchone()[0]

if total_clap_transactions > 0:
    porcentaje_cruzadas = (crossed_count / total_clap_transactions) * 100
    print(f"Porcentaje de transacciones no cruzadas: {100-porcentaje_cruzadas:.2f}%")
else:
    print("No hay transacciones en la tabla CLAP para calcular el porcentaje.")


