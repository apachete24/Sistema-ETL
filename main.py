import dotenv
import os

from crearDB import crearDb
from importarDatos import importarDatos
from calculosDatos import *

# Cargar variables de entorno
dotenv.load_dotenv()
DB_NAME = os.getenv("DB_NAME")
JSON_FILE = os.getenv("JSON_FILE")

# Creacion de la base de datos e importacion de los datos a ella desde el JSON
conn = crearDb(DB_NAME)
importarDatos(JSON_FILE, conn)

getClientesCriticos(conn).show()




# Print resultados finales
'''
resultados = {
    "Número de muestras": num_muestras,
    "Media (valoración >=5)": round(media_valoracion, 2),
    "Desviación (valoración >=5)": round(std_valoracion, 2),
    "Media (incidentes/cliente)": round(media_incidentes, 2),
    "Desviación (incidentes/cliente)": round(std_incidentes, 2),
    "Media horas/incidente": round(media_horas_incidente, 2),
    "Desviación horas/incidente": round(std_horas_incidente, 2),
    "Mínimo horas/empleado": round(min_horas, 2),
    "Máximo horas/empleado": round(max_horas, 2),
    "Mínimo días resolución": min_dias,
    "Máximo días resolución": max_dias,
    "Mínimo incidentes/empleado": min_incidentes_emp,
    "Máximo incidentes/empleado": max_incidentes_emp
}

print("Resultados:")
for k, v in resultados.items():
    print(f"- {k}: {v}")
'''


conn.close()