import json

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
