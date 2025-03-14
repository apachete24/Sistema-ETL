import sqlite3
import pandas as pd
from datetime import datetime

def analizar_fraude_por_agrupaciones():
    # Conexión a la base de datos
    conn = sqlite3.connect('etl_database.db')
    
    # 1. Consulta para obtener datos de "Fraude" (tipo_incidencia = 5)
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
        WHERE t.tipo_incidencia = '5'
    """
    df_fraude = pd.read_sql(query, conn)


 if df_fraude.empty:
        print("No hay incidentes")
        return
     
#Calcular número de contactos por incidente     
contactos_por_incidente = df_fraude.groupby('id_ticket').size().reset_index(name='num_contactos')

#Extraer día de la semana
df_fraude['fecha_apertura'] = pd.to_datetime(df_fraude['fecha_apertura'])
df_fraude['dia_semana'] = df_fraude['fecha_apertura'].dt.day_name()


#Función para análisis estadístico
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

#agrupaciones
agrupaciones = {
        "Por empleado": ['id_emp'],
        "Por nivel de empleado": ['nivel'],
        "Por cliente": ['cliente'],
        "Por tipo de incidente": ['tipo_incidencia'],
        "Por día de la semana": ['dia_semana']
    }
#resultados
resultados = {}
    for nombre, grupo in agrupaciones.items():
        resultados[nombre] = analizar_agrupacion(
            df_fraude.merge(contactos_por_incidente, on='id_ticket'), 
            grupo
        )



if __name__ == "__main__":
    analizar_fraude_por_agrupaciones()
