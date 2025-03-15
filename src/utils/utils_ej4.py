import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

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
