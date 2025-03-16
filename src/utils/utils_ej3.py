import sqlite3
import pandas as pd
from datetime import datetime

def analizar_agrupacion(df, grupo):
    analisis = df.groupby(grupo).agg(
        num_incidentes=('id_ticket', 'nunique'),
        num_contactos=('id_ticket', 'size'),
        media_contactos=('num_contactos', 'mean'),
        mediana_contactos=('num_contactos', 'median'),
        varianza_contactos=('num_contactos', 'var'),
        max_contactos=('num_contactos', 'max'),
        min_contactos=('num_contactos', 'min')
    ).reset_index()
    return analisis

def analizar_fraude_por_agrupaciones():
    # Conexión a la base de datos
    conn = sqlite3.connect('../files/etl_database.db')

    # 1. Consulta corregida: tipo_incidencia como entero
    query = """
        SELECT
            t.id_ticket,
            t.cliente,
            t.fecha_apertura,
            t.tipo_incidencia,
            e.id_emp,
            e.nivel,
            ce.tiempo
        FROM tickets_emitidos t
        JOIN contactos_con_empleados ce ON t.id_ticket = ce.id_ticket
        JOIN empleados e ON ce.id_emp = e.id_emp
        WHERE t.tipo_incidencia = 5
    """
    df_fraude = pd.read_sql(query, conn)

    if df_fraude.empty:
        print("No hay incidentes de tipo 'Fraude' para analizar.")
        conn.close()  # Cerramos conexión aquí también
        return

    # 2. Calcular y fusionar número de contactos por incidente
    contactos_por_incidente = df_fraude.groupby('id_ticket').size().reset_index(name='num_contactos')
    df_fraude = df_fraude.merge(contactos_por_incidente, on='id_ticket')  # Fusión crítica

    # 3. Extraer día de la semana
    df_fraude['fecha_apertura'] = pd.to_datetime(df_fraude['fecha_apertura'])
    df_fraude['dia_semana'] = df_fraude['fecha_apertura'].dt.day_name()

    # Cerrar conexión
    conn.close()

    # 4. Eliminar duplicados para análisis correcto por ticket
    df_tickets = df_fraude.drop_duplicates(subset='id_ticket')

    # 5. Realizar análisis (ejemplo con día de la semana)
    analisis_final = analizar_agrupacion(df_tickets, 'dia_semana')

    agrupaciones = {
        "Por empleado": ['id_emp'],
        "Por nivel de empleado": ['nivel'],
        "Por cliente": ['cliente'],
        "Por tipo de incidente": ['tipo_incidencia'],
        "Por día de la semana": ['dia_semana']
    }

    resultados = {}
    for nombre, grupo in agrupaciones.items():
        resultados[nombre] = analizar_agrupacion(df_fraude, grupo)


    return resultados
