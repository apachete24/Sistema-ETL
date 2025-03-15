
import dotenv
import os
import sys
import pandas as pd
import sqlite3

sys.path.insert(0, 'utils/')

from utils_db import crearDb, importarDatos
from utils_ej2 import *
from utils_ej3 import *
from utils_ej4 import *

DB_NAME = "../files/etl_database.db"
JSON_FILE = "../files/datos.json"

def ejecutar_ejercicio_2(conn):
    print("Ejecutando Ejercicio 2...")

    # 1. Número de muestras totales
    total_muestras = getTotalMuestras(conn)
    print(f"Número de muestras totales: {total_muestras}")

    # 2. Media y desviación estándar de valoración >= 5
    media_valoracion, std_valoracion = getDesviacionSatisfaccion(conn)
    print(f"Media de valoración (>=5): {media_valoracion}")
    print(f"Desviación estándar de valoración (>=5): {std_valoracion}")

    # 3. Media y desviación estándar de incidentes por cliente
    media_incidentes, std_incidentes = getDesviacionIncidentesCliente(conn)
    print(f"Media de incidentes por cliente: {media_incidentes}")
    print(f"Desviación estándar de incidentes por cliente: {std_incidentes}")

    # 4. Media y desviación estándar de horas por incidente
    media_horas_incidente, std_horas_incidente = getDesviacionHorasIncidente(conn)
    print(f"Media de horas por incidente: {media_horas_incidente}")
    print(f"Desviación estándar de horas por incidente: {std_horas_incidente}")

    # 5. Mínimo y máximo de horas trabajadas por empleado
    min_horas, max_horas = getMinMaxHorasTrabajadas(conn)
    print(f"Mínimo de horas trabajadas por empleado: {min_horas}")
    print(f"Máximo de horas trabajadas por empleado: {max_horas}")

    # 6. Mínimo y máximo de tiempo de resolución (en días)
    min_dias, max_dias = getMinMaxTiempoIncidentes(conn)
    print(f"Mínimo de días de resolución: {min_dias}")
    print(f"Máximo de días de resolución: {max_dias}")

    # 7. Mínimo y máximo de incidentes atendidos por empleado
    min_incidentes_emp, max_incidentes_emp = getIncidentesEmpleado(conn)
    print(f"Mínimo de incidentes atendidos por empleado: {min_incidentes_emp}")
    print(f"Máximo de incidentes atendidos por empleado: {max_incidentes_emp}")


def ejecutar_ejercicio_3():

    resultados = analizar_fraude_por_agrupaciones()

    for nombre, df in resultados.items():
        print(f"\n---- {nombre} ----")
        print(df.to_string(index=False))
    print(resultado)

def ejecutar_ejercicio_4(conn):

    getMediaTiempoMantenimiento(conn)


if __name__ == '__main__':

	# Creacion de la base de datos e importacion de los datos a ella desde el JSON
    conn = crearDb(DB_NAME)
    importarDatos(JSON_FILE, conn)

    if len(sys.argv) != 2:
        print("Uso: python script.py <número_de_ejercicio>")


    ejercicio = sys.argv[1]

    try:
        # Ejecutar el ejercicio 2 si el parámetro es "2"
        if ejercicio == "2":
            ejecutar_ejercicio_2(conn)
        elif ejercicio == "3":
            ejecutar_ejercicio_3()
        elif ejercicio == "4":
            ejecutar_ejercicio_4(conn)
        else:
            print("Ejercicio no reconocido. Por favor, use '2' para el ejercicio 2.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cerrar la conexión a la base de datos
        conn.close()
