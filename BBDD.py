import sqlite3
import json
import pandas as pd

# Cargar el archivo JSON
with open('datos.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Conectar a la base de datos SQLite (se creará si no existe)
conn = sqlite3.connect('etl_database.db')
cursor = conn.cursor()

# Para evitar duplicados de los resultados siempre se borra antes de ejecutar este script
cursor.execute("DROP TABLE IF EXISTS contactos_con_empleados")
cursor.execute("DROP TABLE IF EXISTS tickets_emitidos")
cursor.execute("DROP TABLE IF EXISTS tipos_incidentes")
cursor.execute("DROP TABLE IF EXISTS empleados")
cursor.execute("DROP TABLE IF EXISTS clientes")

# Creación de tablas
cursor.execute('''
CREATE TABLE IF NOT EXISTS clientes (
    id_cli TEXT PRIMARY KEY,
    nombre TEXT,
    telefono TEXT,
    provincia TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS empleados (
    id_emp TEXT PRIMARY KEY,
    nombre TEXT,
    nivel INTEGER,
    fecha_contrato TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS tipos_incidentes (
    id_inci TEXT PRIMARY KEY,
    nombre TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS tickets_emitidos (
    id_ticket INTEGER PRIMARY KEY AUTOINCREMENT,
    cliente TEXT,
    fecha_apertura TEXT,
    fecha_cierre TEXT,
    es_mantenimiento INTEGER,
    satisfaccion_cliente INTEGER,
    tipo_incidencia INTEGER,
    FOREIGN KEY (cliente) REFERENCES clientes(id_cli),
    FOREIGN KEY (tipo_incidencia) REFERENCES tipos_incidentes(id_inci)
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS contactos_con_empleados (
    id_contacto INTEGER PRIMARY KEY AUTOINCREMENT,
    id_ticket INTEGER,
    id_emp TEXT,
    fecha TEXT,
    tiempo REAL,
    FOREIGN KEY (id_ticket) REFERENCES tickets_emitidos(id_ticket),
    FOREIGN KEY (id_emp) REFERENCES empleados(id_emp)
)''')

# Inserción de datos en las tablas clientes, empleados y tipos_incidentes
# Tabla Clientes
for cliente in data['clientes']:
    cursor.execute('''INSERT OR IGNORE INTO clientes (id_cli, nombre, telefono, provincia)
    VALUES (?, ?, ?, ?)
    ''', (cliente['id_cli'], cliente['nombre'], cliente['telefono'], cliente['provincia']))

# Tabla Empleados
for empleado in data['empleados']:
    cursor.execute('''INSERT OR IGNORE INTO empleados (id_emp, nombre, nivel, fecha_contrato)
    VALUES (?, ?, ?, ?)
    ''', (empleado['id_emp'], empleado['nombre'], empleado['nivel'], empleado['fecha_contrato']))

# Tabla Tipos_Incidentes
for tipo_incidente in data['tipos_incidentes']:
    cursor.execute('''INSERT OR IGNORE INTO tipos_incidentes (id_inci, nombre)
    VALUES (?, ?)
    ''', (tipo_incidente['id_inci'], tipo_incidente['nombre']))

# Tabla Tickets_Emitidos y Contactos_Empleados, con inserción de IDs automáticos
for ticket in data['tickets_emitidos']:
    cursor.execute('''INSERT INTO tickets_emitidos (cliente, fecha_apertura, fecha_cierre, es_mantenimiento, satisfaccion_cliente, tipo_incidencia)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        ticket['cliente'], ticket['fecha_apertura'], ticket['fecha_cierre'],
        int(ticket['es_mantenimiento']), ticket['satisfaccion_cliente'], ticket['tipo_incidencia']
    ))
    
    id_ticket = cursor.lastrowid #ID automático
    
    for contacto in ticket['contactos_con_empleados']:
        cursor.execute('''INSERT INTO contactos_con_empleados (id_ticket, id_emp, fecha, tiempo)
        VALUES (?, ?, ?, ?)
        ''', (id_ticket, contacto['id_emp'], contacto['fecha'], contacto['tiempo']))

conn.commit()

# CÁLCULOS CON PANDAS
# 1. Número de muestras totales
df_tickets = pd.read_sql("SELECT * FROM tickets_emitidos", conn)
num_muestras = df_tickets.shape[0]

# 2. Media y desviación estándar de valoración >=5
df_filtrado = df_tickets[df_tickets["satisfaccion_cliente"] >= 5]
media_valoracion = df_filtrado["satisfaccion_cliente"].mean()
std_valoracion = df_filtrado["satisfaccion_cliente"].std()

# 3. Media y desviación de incidentes por cliente
incidentes_por_cliente = df_tickets.groupby("cliente").size()
media_incidentes = incidentes_por_cliente.mean()
std_incidentes = incidentes_por_cliente.std()

# 4. Media y desviación de horas por incidente 
df_horas_por_incidente = pd.read_sql("""
    SELECT id_ticket, SUM(tiempo) AS total_horas 
    FROM contactos_con_empleados  
    GROUP BY id_ticket
""", conn)
media_horas_incidente = df_horas_por_incidente["total_horas"].mean()
std_horas_incidente = df_horas_por_incidente["total_horas"].std()

# 5. Horas trabajadas por empleado
df_horas = pd.read_sql("""
    SELECT id_emp, SUM(tiempo) AS total_horas 
    FROM contactos_con_empleados  
    GROUP BY id_emp
""", conn)
min_horas = df_horas["total_horas"].min()
max_horas = df_horas["total_horas"].max()

# 6. Tiempo de resolución (días entre apertura y cierre)
df_tickets["tiempo_resolucion"] = (
    pd.to_datetime(df_tickets["fecha_cierre"]) 
    - pd.to_datetime(df_tickets["fecha_apertura"])
).dt.days
min_dias = df_tickets["tiempo_resolucion"].min()
max_dias = df_tickets["tiempo_resolucion"].max()

# 7. Incidentes atendidos por empleado 
df_incidentes_empleado = pd.read_sql("""
    SELECT id_emp, COUNT(DISTINCT id_ticket) AS total_incidentes 
    FROM contactos_con_empleados  
    GROUP BY id_emp
""", conn)
min_incidentes_emp = df_incidentes_empleado["total_incidentes"].min()
max_incidentes_emp = df_incidentes_empleado["total_incidentes"].max()

# Resultados finales (actualizado)
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

conn.close()
