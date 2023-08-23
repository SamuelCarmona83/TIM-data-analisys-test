import psycopg2
import io
import os
import csv
from datetime import datetime
from dotenv import load_dotenv

# Parámetros de la base de datos

# Cargar las variables de entorno desde el archivo .env
load_dotenv()
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
drop_table_query = "DROP TABLE IF EXISTS bansur"
cursor.execute(drop_table_query)
connection.commit()

# Create the table
create_table_query = """
CREATE TABLE bansur (
    TARJETA TEXT,
    TIPO_TRX TEXT,
    MONTO NUMERIC,
    FECHA_TRANSACCION DATE,
    CODIGO_AUTORIZACION TEXT,
    ID_ADQUIRIENTE TEXT,
    FECHA_RECEPCION DATE
);
"""
cursor.execute(create_table_query)
connection.commit()

# Import data
# Import data
# Open the CSV file using csv.reader
with open('./scripts/csv_files/BANSUR.csv', 'r') as f:
    csv_reader = csv.reader(f)
    next(csv_reader)  # Skip the header row

    for row in csv_reader:
        for row in csv_reader:
            # Convert numerical values to appropriate types
            tarjeta = row[0]
            tipo_trx = row[1]
            monto = float(row[2])  # Convert to float
            fecha_transaccion = datetime.strptime(row[3], '%Y%m%d').date()
            codigo_autorizacion = row[4]
            id_adquiriente = row[5]
            fecha_recepcion = datetime.strptime(row[6], '%Y-%m-%d').date()

            # Prepare the row for insertion
            corrected_row = [tarjeta, tipo_trx, monto, fecha_transaccion, codigo_autorizacion, id_adquiriente, fecha_recepcion]

            # Convert the corrected row to a CSV string
            csv_line = io.StringIO(','.join(str(value) if value is not None else '' for value in corrected_row))

            # Use the CSV line to load into the database
            cursor.copy_expert("COPY bansur FROM stdin WITH CSV", csv_line)


connection.commit()
cursor.close()
connection.close()