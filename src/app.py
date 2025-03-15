from flask import Flask, render_template
import sqlite3
import sys
import os

sys.path.insert(0, 'utils/')

from utils_db import crearDb, importarDatos
from utils_ej2 import *
from utils_ej3 import analizar_fraude_por_agrupaciones
from utils_ej4 import *

app = Flask(__name__, static_folder=os.path.abspath("../static"))
DB_NAME = "../files/etl_database.db"
JSON_FILE = "../files/datos.json"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/ejercicio2')
def ejercicio2():
    conn = get_db_connection()
    try:
        data = {
            'total_muestras': getTotalMuestras(conn),
            'media_valoracion': round(getDesviacionSatisfaccion(conn)[0], 2),
            'std_valoracion': round(getDesviacionSatisfaccion(conn)[1], 2),
            'media_incidentes': round(getDesviacionIncidentesCliente(conn)[0], 2),
            'std_incidentes': round(getDesviacionIncidentesCliente(conn)[1], 2),
            'media_horas': round(getDesviacionHorasIncidente(conn)[0], 2),
            'std_horas': round(getDesviacionHorasIncidente(conn)[1], 2),
            'min_horas': round(getMinMaxHorasTrabajadas(conn)[0], 2),
            'max_horas': round(getMinMaxHorasTrabajadas(conn)[1], 2),
            'min_dias': getMinMaxTiempoIncidentes(conn)[0],
            'max_dias': getMinMaxTiempoIncidentes(conn)[1],
            'min_incidentes_emp': getIncidentesEmpleado(conn)[0],
            'max_incidentes_emp': getIncidentesEmpleado(conn)[1]
        }
        return render_template('ejercicio2.html', data=data)
    finally:
        conn.close()

@app.route('/ejercicio3')
def ejercicio3():
    conn = get_db_connection()
    try:
        resultados = analizar_fraude_por_agrupaciones()
        return render_template('ejercicio3.html', 
                            resultados=resultados,
                            tipos_agrupacion=resultados.keys())
    finally:
        conn.close()

@app.route('/ejercicio4')
def ejercicio4():
    conn = get_db_connection()
    try:
        # Generar gráficos usando tus funciones originales
        getMediaTiempoMantenimiento(conn)
        getTipoDeIncidente(conn)
        getClientesCriticos(conn)
        getActuacionesEmpleados(conn)
        getActuacionesDiaSemana(conn)

        # Rutas de los gráficos generados (asumiendo que guardan en static/img)
        graficos = {
            'media_tiempo': 'img/media_tiempo.png',
            'boxplot': 'img/boxplot.png',
            'clientes_criticos': 'img/clientes_criticos.png',
            'actuaciones_empleados': 'img/actuaciones_empleados.png',
            'actuaciones_dias': 'img/actuaciones_dias.png'
        }
        return render_template('ejercicio4.html', graficos=graficos)
    finally:
        conn.close()

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    # Inicializar DB solo si no existe
    try:
        conn = sqlite3.connect(DB_NAME)
    except sqlite3.OperationalError:
        crearDb(DB_NAME)
        importarDatos(JSON_FILE, conn)
    finally:
        if conn:
            conn.close()
    app.run(debug=True, port=5000)
