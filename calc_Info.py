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




if __name__ == "__main__":
    analizar_fraude_por_agrupaciones()
