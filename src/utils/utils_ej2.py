import pandas as pd
import matplotlib.pyplot as plt

# EJERCICIO 2

# 1. Número de muestras totales
def getTotalMuestras(conn):
    df_tickets = pd.read_sql("SELECT * FROM tickets_emitidos", conn)
    num_muestras = df_tickets.shape[0]

    return num_muestras


# 2. Media y desviación estándar de valoración >=5
def getDesviacionSatisfaccion(conn):
    df_tickets = pd.read_sql("SELECT * FROM tickets_emitidos", conn)
    df_filtrado = df_tickets[df_tickets["satisfaccion_cliente"] >= 5]
    media_valoracion = df_filtrado["satisfaccion_cliente"].mean()
    std_valoracion = df_filtrado["satisfaccion_cliente"].std()

    return media_valoracion, std_valoracion


# 3. Media y desviación de incidentes por cliente
def getDesviacionIncidentesCliente(conn):
    df_tickets = pd.read_sql("SELECT * FROM tickets_emitidos", conn)

    incidentes_por_cliente = df_tickets.groupby("cliente").size()
    media_incidentes = incidentes_por_cliente.mean()
    std_incidentes = incidentes_por_cliente.std()

    return media_incidentes, std_incidentes


# 4. Media y desviación de horas por incidente
def getDesviacionHorasIncidente(conn):

    df_horas_por_incidente = pd.read_sql("""
        SELECT id_ticket, SUM(tiempo) AS total_horas 
        FROM contactos_con_empleados  
        GROUP BY id_ticket
    """, conn)

    media_horas_incidente = df_horas_por_incidente["total_horas"].mean()
    std_horas_incidente = df_horas_por_incidente["total_horas"].std()

    return media_horas_incidente, std_horas_incidente


# 5. Horas trabajadas por empleado
def getMinMaxHorasTrabajadas(conn):

    df_horas = pd.read_sql("""
        SELECT id_emp, SUM(tiempo) AS total_horas 
        FROM contactos_con_empleados  
        GROUP BY id_emp
    """, conn)

    min_horas = df_horas["total_horas"].min()
    max_horas = df_horas["total_horas"].max()

    return min_horas, max_horas


# 6. Tiempo de resolución (días entre apertura y cierre)
def getMinMaxTiempoIncidentes(conn):
    df_tickets = pd.read_sql("SELECT * FROM tickets_emitidos", conn)
    df_tickets["tiempo_resolucion"] = (
            pd.to_datetime(df_tickets["fecha_cierre"])
            - pd.to_datetime(df_tickets["fecha_apertura"])
    ).dt.days
    min_dias = df_tickets["tiempo_resolucion"].min()
    max_dias = df_tickets["tiempo_resolucion"].max()

    return min_dias, max_dias


# 7. Incidentes atendidos por empleado
def getIncidentesEmpleado(conn):
    df_incidentes_empleado = pd.read_sql("""
        SELECT id_emp, COUNT(DISTINCT id_ticket) AS total_incidentes 
        FROM contactos_con_empleados  
        GROUP BY id_emp
    """, conn)
    min_incidentes_emp = df_incidentes_empleado["total_incidentes"].min()
    max_incidentes_emp = df_incidentes_empleado["total_incidentes"].max()

    return min_incidentes_emp, max_incidentes_emp
