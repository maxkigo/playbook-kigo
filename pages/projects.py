import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import plotly.express as px
from google.oauth2 import service_account


st.set_page_config(
    page_title="Kigo - Stickiness",
    layout="wide"
)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.write()

with col2:
    st.image('https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/logos.png')

with col3:
    st.title('Kigo Projects')

with col4:
    st.write()

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=3600)
def proyects_activos():
    proyectos = """
    WITH proyectos_activiti AS (
    SELECT 
        EXTRACT(MONTH FROM TIMESTAMP_ADD(TED.paymentDate, INTERVAL -6 HOUR)) AS month, 
        (COUNT(TED.transactionId) * 2) AS operaciones, 
        CATED.parkingLotName AS proyecto
    FROM 
        `parkimovil-app`.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS TED
    JOIN 
        `parkimovil-app`.cargomovil_pd.PKM_PARKING_LOT_CAT CATED
    ON 
        TED.parkingLotId = CATED.id
    WHERE 
        EXTRACT(YEAR FROM TIMESTAMP_ADD(TED.paymentDate, INTERVAL -6 HOUR)) = 2024 
        AND EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL -6 HOUR)) >= '2024-01-01'
    GROUP BY 
        month, CATED.parkingLotName

    UNION ALL

    SELECT 
        EXTRACT(MONTH FROM TIMESTAMP_ADD(TPV.date, INTERVAL -6 HOUR)) AS month, 
        COUNT(TPV.transactionId) AS operaciones, 
        PVCAT.name AS proyecto
    FROM 
        `parkimovil-app`.cargomovil_pd.PKM_TRANSACTION TPV
    JOIN 
        `parkimovil-app`.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT PVCAT
    ON 
        TPV.zoneId = PVCAT.id
    WHERE 
        EXTRACT(YEAR FROM TIMESTAMP_ADD(TPV.date, INTERVAL -6 HOUR)) = 2024 
        AND EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL -6 HOUR)) >= '2024-01-01'
    GROUP BY 
        month, PVCAT.name

    UNION ALL

    SELECT 
        EXTRACT(MONTH FROM S.date) AS month, 
        COUNT(S.function_) AS operaciones, 
        R.alias AS proyecto
    FROM 
        `parkimovil-app`.geosek_raspis.log_sek S
    JOIN 
        `parkimovil-app`.geosek_raspis.raspis R
    ON 
        S.QR = R.qr
    WHERE 
        EXTRACT(YEAR FROM S.date) = 2024 
        AND EXTRACT(DATE FROM date) >= '2024-01-01'
    GROUP BY 
        month, R.alias
),

filtered_proyectos AS (
    SELECT 
        month,
        proyecto,
        SUM(operaciones) AS total_operaciones
    FROM 
        proyectos_activiti
    GROUP BY 
        month, proyecto
    HAVING 
        total_operaciones > 100
)

SELECT 
    proyecto,
    IFNULL(SUM(IF(month = 1, total_operaciones, NULL)), 0) AS Enero,
    IFNULL(SUM(IF(month = 2, total_operaciones, NULL)), 0) AS Febrero,
    IFNULL(SUM(IF(month = 3, total_operaciones, NULL)), 0) AS Marzo,
    IFNULL(SUM(IF(month = 4, total_operaciones, NULL)), 0) AS Abril,
    IFNULL(SUM(IF(month = 5, total_operaciones, NULL)), 0) AS Mayo,
    IFNULL(SUM(IF(month = 6, total_operaciones, NULL)), 0) AS Junio,
    IFNULL(SUM(IF(month = 7, total_operaciones, NULL)), 0) AS Julio,
    IFNULL(SUM(IF(month = 8, total_operaciones, NULL)), 0) AS Agosto,
    IFNULL(SUM(IF(month = 9, total_operaciones, NULL)), 0) AS Septiembre,
    IFNULL(SUM(IF(month = 10, total_operaciones, NULL)), 0) AS Octubre,
    IFNULL(SUM(IF(month = 11, total_operaciones, NULL)), 0) AS Noviembre,
    IFNULL(SUM(IF(month = 12, total_operaciones, NULL)), 0) AS Diciembre
FROM 
    filtered_proyectos
GROUP BY 
    proyecto
ORDER BY 
    proyecto
"""

    proyectos_activos_pivot = client.query(proyectos).to_dataframe()
    return proyectos_activos_pivot

@st.cache_data(ttl=3600)
def proyectos_errores():
    count_error = """
        SELECT EXTRACT(YEAR FROM date) AS year,
               EXTRACT(WEEK FROM date) AS week,
               SUM(CASE WHEN function_ LIKE 'open%error%500' OR function_
               IN ('open_error_', 'open_error') THEN 1 END) AS errores_desconexion,
               SUM(CASE WHEN function_ LIKE '%501%' THEN 1 END) AS errores_presencia
        FROM (
            SELECT date, function_ FROM parkimovil-app.geosek_raspis.log L
            JOIN `parkimovil-app`.geosek_raspis.raspis R
            ON L.QR = R.qr
            UNION ALL
            SELECT date, function_ FROM parkimovil-app.geosek_raspis.log_sek LS
            JOIN `parkimovil-app`.geosek_raspis.raspis R
            ON LS.QR = R.qr
        ) AS all_logs
            WHERE EXTRACT(YEAR FROM date) >= 2024
            AND EXTRACT(WEEK FROM date)
            <= EXTRACT(WEEK FROM CURRENT_DATE("America/Mexico_City"))
        GROUP BY year, week
        ORDER BY year, week
    """
    df_proyectos_errores = client.query(count_error).to_dataframe()
    return df_proyectos_errores


@st.cache_data(ttl=3600)
def proyectos_lecturas():
    query = """
    SELECT EXTRACT(YEAR FROM date) AS year,
               EXTRACT(WEEK FROM date) AS week,
               SUM(CASE WHEN function_ IN ('open', 'open_rf', 'open_qr') THEN 1 END) AS lecturas_exitosas,
               SUM(CASE WHEN function_ LIKE 'open%error%500' OR function_ IN ('open_error_', 'open_error') THEN 1 END) AS errores_desconexion,
               SUM(CASE WHEN function_ LIKE '%501%' THEN 1 END) AS errores_presencia,
               SUM(CASE WHEN function_ LIKE 'denied%' THEN 1 END) AS accesos_denegados
        FROM (
            SELECT date, function_ FROM parkimovil-app.geosek_raspis.log L
            JOIN `parkimovil-app`.geosek_raspis.raspis R
            ON L.QR = R.qr
            UNION ALL
            SELECT date, function_ FROM parkimovil-app.geosek_raspis.log_sek LS
            JOIN `parkimovil-app`.geosek_raspis.raspis R
            ON LS.QR = R.qr
        ) AS all_logs
        WHERE EXTRACT(YEAR FROM date) >= 2024
        GROUP BY year, week
        ORDER BY year, week
    """

    df_lecturas = client.query(query).to_dataframe()
    return df_lecturas


df_errores = proyectos_errores()
proyectos_activos_pivot = proyects_activos()
df_lecturas = proyectos_lecturas()

# Crear figura de Plotly
fig_errores = go.Figure()

# Agregar barras para errores de desconexión
fig_errores.add_trace(go.Bar(x=df_errores['week'], y=df_errores['errores_desconexion'],
                     name='Errores de Desconexión'))

# Agregar barras para errores de presencia
fig_errores.add_trace(go.Bar(x=df_errores['week'], y=df_errores['errores_presencia'],
                     name='Errores de Presencia'))

# Actualizar el diseño del gráfico
fig_errores.update_layout(title='Errores Semanales',
                  xaxis_title='Semana',
                  yaxis_title='Cantidad de Errores',
                  barmode='group')

@st.cache_data(ttl=3600)
def create_stacked_bar_chart(df):
    # Calcular los totales por semana
    df['total'] = df[['lecturas_exitosas', 'errores_desconexion', 'errores_presencia', 'accesos_denegados']].sum(axis=1)

    # Calcular los porcentajes
    df['lecturas_exitosas_pct'] = df['lecturas_exitosas'] / df['total'] * 100
    df['errores_desconexion_pct'] = df['errores_desconexion'] / df['total'] * 100
    df['errores_presencia_pct'] = df['errores_presencia'] / df['total'] * 100
    df['accesos_denegados_pct'] = df['accesos_denegados'] / df['total'] * 100

    # Preparar los datos para Plotly
    fig = go.Figure()

    # Agregar las barras apiladas y los porcentajes dentro de las barras con valores reales en hover
    fig.add_trace(go.Bar(x=df['week'], y=df['lecturas_exitosas_pct'], name='Lecturas Exitosas',
                         text=df['lecturas_exitosas_pct'].round(2).astype(str) + '%',
                         hovertext=df['lecturas_exitosas'].astype(str),
                         textposition='inside'))
    fig.add_trace(go.Bar(x=df['week'], y=df['errores_desconexion_pct'], name='Errores Desconexión',
                         text=df['errores_desconexion_pct'].round(2).astype(str) + '%',
                         hovertext=df['errores_desconexion'].astype(str),
                         textposition='inside'))
    fig.add_trace(go.Bar(x=df['week'], y=df['errores_presencia_pct'], name='Errores Presencia',
                         text=df['errores_presencia_pct'].round(2).astype(str) + '%',
                         hovertext=df['errores_presencia'].astype(str),
                         textposition='inside'))
    fig.add_trace(go.Bar(x=df['week'], y=df['accesos_denegados_pct'], name='Accesos Denegados',
                         text=df['accesos_denegados_pct'].round(2).astype(str) + '%',
                         hovertext=df['accesos_denegados'].astype(str),
                         textposition='inside'))

    # Actualizar diseño del gráfico
    fig.update_layout(title='Porcentaje de Tipos de Lecturas Semanal',
                      xaxis_title='Semana del Año',
                      yaxis_title='Porcentaje (%)',
                      barmode='stack')

    return fig


fig_stacked_bar_chart = create_stacked_bar_chart(df_lecturas)

st.plotly_chart(fig_errores, use_container_width=True)
st.plotly_chart(fig_stacked_bar_chart, use_container_width=True)
