import psycopg2
import os
import io
import csv
from datetime import datetime
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

# Conexión a la base de datos
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Drop the existing table if it exists
drop_table_query = "DROP TABLE IF EXISTS clap"
cursor.execute(drop_table_query)
connection.commit()

# Create the table
create_table_query = """
CREATE TABLE clap (
    INICIO6_TARJETA TEXT,
    FINAL4_TARJETA TEXT,
    TIPO_TRX TEXT,
    MONTO NUMERIC,
    FECHA_TRANSACCION DATE,
    CODIGO_AUTORIZACION TEXT,
    ID_BANCO TEXT,
    FECHA_RECEPCION_BANCO DATE
);
"""
cursor.execute(create_table_query)
connection.commit()

# Import data
# Import data
# Open the CSV file using csv.reader
with open('./scripts/csv_files/CLAP.csv', 'r') as f:
    csv_reader = csv.reader(f)
    next(csv_reader)  # Skip the header row

    for row in csv_reader:
        # Modify the row if needed
        # For example, replace empty values with a placeholder
        corrected_row = [value if value else 'TEMPORAL_EMPTY_VALUE' for value in row]

        fecha_sin_hora = datetime.strptime(corrected_row[4], '%Y-%m-%d %H:%M:%S.%f').date()

        fecha_formateada = fecha_sin_hora.strftime('%Y%m%d')

        corrected_row[4] = fecha_formateada

        # Convert the corrected row to a CSV string
        csv_line = io.StringIO(','.join(corrected_row))

        # Use the CSV line to load into the database
        cursor.copy_expert("COPY clap FROM stdin WITH CSV", csv_line)


connection.commit()
cursor.close()
connection.close()