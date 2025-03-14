import dotenv
import os

from crearDB import crearDb
from importarDatos import importarDatos

# Cargar variables de entorno
dotenv.load_dotenv()
DB_NAME = os.getenv("DB_NAME")
JSON_FILE = os.getenv("JSON_FILE")


conn = crearDb(DB_NAME)

importarDatos(JSON_FILE, conn)