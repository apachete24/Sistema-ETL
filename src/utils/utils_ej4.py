import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def getTiemposData(conn):

    query = """
        SELECT es_mantenimiento, fecha_apertura, fecha_cierre, tipo_incidencia
        FROM tickets_emitidos
    """
    df = pd.read_sql(query, conn)
    df["tiempo_resolucion"] = (
        pd.to_datetime(df["fecha_cierre"]) - pd.to_datetime(df["fecha_apertura"])
    ).dt.days
    return df

def getMediaTiempoMantenimiento(conn):
    # Obtener datos desde la base de datos
    df_tickets = pd.read_sql("SELECT * FROM tickets_emitidos", conn)

    # Calcular el tiempo de resolución en días
    df_tickets["tiempo_resolucion"] = (
        pd.to_datetime(df_tickets["fecha_cierre"]) - pd.to_datetime(df_tickets["fecha_apertura"])
    ).dt.days

    # Calcular medias
    media_mantenimiento = df_tickets[df_tickets["es_mantenimiento"] == True]["tiempo_resolucion"].mean()
    media_no_mantenimiento = df_tickets[df_tickets["es_mantenimiento"] == False]["tiempo_resolucion"].mean()

    # Crear gráfico de barras
    categorias = ["Mantenimiento", "No Mantenimiento"]
    valores = [media_mantenimiento, media_no_mantenimiento]

    plt.figure(figsize=(6, 4))
    plt.bar(categorias, valores, color=["blue", "orange"])
    plt.xlabel("Tipo de Incidente")
    plt.ylabel("Días promedio de resolución")
    plt.title("Tiempo medio de resolución de incidentes")
    plt.ylim(0, max(valores) + 2)  # Ajustar el límite del eje Y para mayor claridad
    plt.show()

def getTipoDeIncidente(conn):

    df = getTiemposData(conn)
    tipos_incidente = df["tipo_incidencia"].unique()
    datos_boxplot = [
        df[df["tipo_incidencia"] == tipo]["tiempo_resolucion"]
        for tipo in tipos_incidente
    ]

    plt.figure(figsize=(12, 6))
    plt.boxplot(
        datos_boxplot,
        whis=[5, 95],  # Se usan percentiles 5% y 95%
        labels=tipos_incidente,
        showfliers=False
    )
    plt.title("Tiempos de resolución por tipo de incidente (P5-P95)")
    plt.xlabel("Tipo de incidente")
    plt.ylabel("Días de resolución")
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.show()

def getClientesCriticos(conn):

    query = """
        SELECT cliente, COUNT(*) AS total_incidentes 
        FROM tickets_emitidos 
        WHERE es_mantenimiento = 1 AND tipo_incidencia != '1'
        GROUP BY cliente
        ORDER BY total_incidentes DESC
        LIMIT 5
    """
    df_clientes = pd.read_sql(query, conn)

    plt.figure(figsize=(10, 5))
    plt.bar(
        df_clientes["cliente"].astype(str),
        df_clientes["total_incidentes"],
        color="#2ca02c"
    )
    plt.title("Top 5 clientes críticos (mantenimiento y tipo ≠ 1)")
    plt.xlabel("Cliente")
    plt.ylabel("Total incidentes")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()

def getActuacionesEmpleados(conn):

    query = """
        SELECT id_emp, COUNT(*) AS total_actuaciones 
        FROM contactos_con_empleados 
        GROUP BY id_emp
    """
    df_empleados = pd.read_sql(query, conn)

    plt.figure(figsize=(14, 6))
    plt.bar(
        df_empleados["id_emp"].astype(str),
        df_empleados["total_actuaciones"],
        color="#d62728"
    )
    plt.title("Total de actuaciones por empleado")
    plt.xlabel("ID Empleado")
    plt.ylabel("Número de actuaciones")
    plt.xticks(rotation=45)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()

def getActuacionesDiaSemana(conn):

    query = """
        SELECT fecha, COUNT(*) AS total_actuaciones 
        FROM contactos_con_empleados 
        GROUP BY fecha
    """
    df_dias = pd.read_sql(query, conn)
    df_dias["dia_semana"] = pd.to_datetime(df_dias["fecha"]).dt.day_name()

    dias_orden = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    df_dias["dia_semana"] = pd.Categorical(df_dias["dia_semana"], categories=dias_orden, ordered=True)
    df_dias = df_dias.groupby("dia_semana")["total_actuaciones"].sum().reset_index()

    plt.figure(figsize=(12, 6))
    plt.bar(
        df_dias["dia_semana"],
        df_dias["total_actuaciones"],
        color="#9467bd"
    )
    plt.title("Total de actuaciones por día de la semana")
    plt.xlabel("Día de la semana")
    plt.ylabel("Total actuaciones")
    plt.xticks(rotation=45)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()
