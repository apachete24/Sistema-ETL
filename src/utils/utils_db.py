import sqlite3
import json

# CREACION DE LA BASE DE DATOS
def crearDb(DB_NAME):

    # Conectar a la base de datos SQLite (se creará si no existe)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Para evitar registro duplicados siempre se borra antes
    cursor.execute("DROP TABLE IF EXISTS contactos_con_empleados")
    cursor.execute("DROP TABLE IF EXISTS tickets_emitidos")
    cursor.execute("DROP TABLE IF EXISTS tipos_incidentes")
    cursor.execute("DROP TABLE IF EXISTS empleados")
    cursor.execute("DROP TABLE IF EXISTS clientes")

    # Crear las tablas
    cursor.execute('''
    CREATE TABLE clientes (
        id_cli INTEGER PRIMARY KEY,
        nombre TEXT,
        telefono TEXT,
        provincia TEXT
    )
    ''')


    cursor.execute('''
    CREATE TABLE empleados (
        id_emp INTEGER PRIMARY KEY,
        nombre TEXT,
        nivel INTEGER,
        fecha_contrato TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE tipos_incidentes (
        id_inci INTEGER PRIMARY KEY,
        nombre TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE tickets_emitidos (
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
    CREATE TABLE contactos_con_empleados (
        id_contacto INTEGER PRIMARY KEY AUTOINCREMENT,
        id_ticket INTEGER,
        id_emp TEXT,
        fecha TEXT,
        tiempo REAL,
        FOREIGN KEY (id_ticket) REFERENCES tickets_emitidos(id_ticket),
        FOREIGN KEY (id_emp) REFERENCES empleados(id_emp)
    )
    ''')

    return conn

def importarDatos(JSON_FILE, conn):

    cursor = conn.cursor()
    # Cargar el archivo JSON
    with open(JSON_FILE, 'r', encoding='utf-8') as file:
        data = json.load(file)

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

        id_ticket = cursor.lastrowid  # ID automático

        for contacto in ticket['contactos_con_empleados']:
            cursor.execute('''INSERT INTO contactos_con_empleados (id_ticket, id_emp, fecha, tiempo)
            VALUES (?, ?, ?, ?)
            ''', (id_ticket, contacto['id_emp'], contacto['fecha'], contacto['tiempo']))

    conn.commit()
