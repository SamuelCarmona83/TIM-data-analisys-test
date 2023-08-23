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

# Consulta única para obtener datos de ambas tablas
query_combined = """
    SELECT
        'clap' AS source,
        INICIO6_TARJETA || FINAL4_TARJETA AS tarjeta,
        MONTO,
        FECHA_TRANSACCION
    FROM clap
    UNION ALL
    SELECT
        'bansur' AS source,
        TARJETA,
        MONTO,
        FECHA_TRANSACCION
    FROM bansur
"""

with psycopg2.connect(**db_params) as conn:
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
        cursor.execute(query_combined)
        all_transacciones = cursor.fetchall()

transacciones_bansur_dict = {trans['tarjeta']: trans for trans in all_transacciones if trans['source'] == 'bansur'}
transacciones_cruzadas = 0
transacciones_totales = len([trans for trans in all_transacciones if trans['source'] == 'clap'])

for clap_transaccion in all_transacciones:
    if clap_transaccion['source'] == 'clap':
        clap_tarjeta = clap_transaccion['tarjeta']
        clap_monto = clap_transaccion['monto']
        clap_fecha = clap_transaccion['fecha_transaccion']
        
        if clap_tarjeta in transacciones_bansur_dict:
            bansur_transaccion = transacciones_bansur_dict[clap_tarjeta]
            bansur_monto = bansur_transaccion['monto']
            bansur_fecha = bansur_transaccion['fecha_transaccion']
            
            if abs(clap_monto - bansur_monto) <= 0.99 and clap_fecha == bansur_fecha:
                transacciones_cruzadas += 1

if transacciones_totales > 0:
    porcentaje_cruzadas = (transacciones_cruzadas / transacciones_totales) * 100
    print(f"Porcentaje de transacciones cruzadas: {porcentaje_cruzadas:.2f}%")
else:
    print("No hay transacciones para calcular el porcentaje.")
