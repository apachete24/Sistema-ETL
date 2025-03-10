import sqlite3

# Conectar a la base de datos SQLite (se creará si no existe)
conn = sqlite3.connect('etl_database.db')
cursor = conn.cursor()

# Crear las tablas
cursor.execute('''
CREATE TABLE IF NOT EXISTS clientes (
    id_cli INTEGER PRIMARY KEY,
    nombre TEXT,
    telefono TEXT,
    provincia TEXT
)
''')


cursor.execute('''
CREATE TABLE IF NOT EXISTS empleados (
    id_emp INTEGER PRIMARY KEY,
    nombre TEXT,
    nivel INTEGER,
    fecha_contrato TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS tipos_incidentes (
    id_inci INTEGER PRIMARY KEY,
    nombre TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS tickets_emitidos (
    id_ticket INTEGER PRIMARY KEY AUTOINCREMENT,
    cliente TEXT,
    fecha_apertura TEXT,
    fecha_cierre TEXT,
    es_mantenimiento INTEGER,
    satisfaccion_cliente INTEGER,
    tipo_incidencia TEXT,
    FOREIGN KEY (cliente) REFERENCES clientes(id_cli),
    FOREIGN KEY (tipo_incidencia) REFERENCES tipos_incidentes(id_inci)
)
''')

cursor.execute(
'''
CREATE TABLE IF NOT EXISTS contactos_con_empleados (
    id_contacto INTEGER PRIMARY KEY AUTOINCREMENT,
    id_ticket INTEGER,
    id_emp TEXT,
    fecha TEXT,
    tiempo REAL,
    FOREIGN KEY (id_ticket) REFERENCES tickets_emitidos(id_ticket),
    FOREIGN KEY (id_emp) REFERENCES empleados(id_emp)
)
''')