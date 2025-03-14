import sqlite3


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