import streamlit as st
import requests
import pandas as pd
import time

# Configuración
st.set_page_config(page_title="Dashboard de Caché", layout="wide")
st.title("Monitor en Tiempo Real - Sistema de Caché")

URL_METRICAS = "http://sistema_metricas:8000/stats"

placeholder = st.empty()

# Bucle infinito
while True:
    try:
        response = requests.get(URL_METRICAS)
        if response.status_code == 200:
            datos = response.json()
            
            with placeholder.container():
                col1, col2, col3, col4 = st.columns(4)
                col1.metric(label="Total Peticiones", value=datos["total_eventos"])
                
                hit_rate_pct = datos["hit_rate"] * 100
                col2.metric(label="Hit Rate", value=f"{hit_rate_pct:.2f}%")
                
                col3.metric(label="Latencia Promedio", value=f"{datos['latencia_promedio_ms']} ms")
                col4.metric(label="Hits / Misses", value=f"{datos['hits']} / {datos['misses']}")
                
                st.markdown("---")
                
                # Gráfico de Barras: Hits vs Misses
                st.subheader("Comparación: Cache Hits vs Cache Misses")
                df_resultados = pd.DataFrame({
                    "Tipo": ["Hits", "Misses"],
                    "Cantidad": [datos["hits"], datos["misses"]]
                })
                st.bar_chart(df_resultados.set_index("Tipo"), color=["#17C37B"])
                
    except requests.exceptions.ConnectionError:
        with placeholder.container():
            st.warning("Esperando conexión con el Sistema de Métricas... Asegúrate de que el contenedor esté corriendo.")
            
    # Esperamos 2 segundos
    time.sleep(2)