import sqlite3
import json

# Cargar el archivo JSON
with open('datos.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Conectar a la base de datos SQLite (se creará si no existe)
conn = sqlite3.connect('etl_database.db')
cursor = conn.cursor()

# Crear las tablas
cursor.execute('''
CREATE TABLE IF NOT EXISTS clientes (
    id_cli TEXT PRIMARY KEY,
    nombre TEXT,
    telefono TEXT,
    provincia TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS empleados (
    id_emp TEXT PRIMARY KEY,
    nombre TEXT,
    nivel INTEGER,
    fecha_contrato TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS tipos_incidentes (
    id_inci TEXT PRIMARY KEY,
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

cursor.execute('''
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

# Insertar datos en la tabla `clientes`
for cliente in data['clientes']:
    cursor.execute('''
    INSERT INTO clientes (id_cli, nombre, telefono, provincia)
    VALUES (?, ?, ?, ?)
    ''', (cliente['id_cli'], cliente['nombre'], cliente['telefono'], cliente['provincia']))

# Insertar datos en la tabla `empleados`
for empleado in data['empleados']:
    cursor.execute('''
    INSERT INTO empleados (id_emp, nombre, nivel, fecha_contrato)
    VALUES (?, ?, ?, ?)
    ''', (empleado['id_emp'], empleado['nombre'], empleado['nivel'], empleado['fecha_contrato']))

# Insertar datos en la tabla `tipos_incidentes`
for tipo_incidente in data['tipos_incidentes']:
    cursor.execute('''
    INSERT INTO tipos_incidentes (id_inci, nombre)
    VALUES (?, ?)
    ''', (tipo_incidente['id_inci'], tipo_incidente['nombre']))

# Insertar datos en la tabla `tickets_emitidos` y `contactos_con_empleados`
for ticket in data['tickets_emitidos']:
    cursor.execute('''
    INSERT INTO tickets_emitidos (cliente, fecha_apertura, fecha_cierre, es_mantenimiento, satisfaccion_cliente, tipo_incidencia)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (ticket['cliente'], ticket['fecha_apertura'], ticket['fecha_cierre'],
          int(ticket['es_mantenimiento']), ticket['satisfaccion_cliente'], ticket['tipo_incidencia']))

    # Obtener el último ID insertado en `tickets_emitidos`
    id_ticket = cursor.lastrowid

    # Insertar los contactos con empleados
    for contacto in ticket['contactos_con_empleados']:
        cursor.execute('''
        INSERT INTO contactos_con_empleados (id_ticket, id_emp, fecha, tiempo)
        VALUES (?, ?, ?, ?)
        ''', (id_ticket, contacto['id_emp'], contacto['fecha'], contacto['tiempo']))

# Guardar los cambios y cerrar la conexión
conn.commit()
conn.close()

print("Base de datos creada y datos importados exitosamente.")