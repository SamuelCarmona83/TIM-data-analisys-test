from flask import Flask
import psycopg2

app = Flask(__name__)

@app.route('/')
def hello():
    return "Hello from Flask!"

@app.route('/database')
def database():
    conn = psycopg2.connect(
        host='postgres',
        user='myuser',
        password='mypassword',
        dbname='mydb'
    )
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM your_table')
    data = cursor.fetchall()
    conn.close()
    return str(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
