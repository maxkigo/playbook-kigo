import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import plotly.express as px
from statsmodels.tsa.arima.model import ARIMA
from google.oauth2 import service_account


st.set_page_config(
    page_title="Kigo - Zonas Metropolitanas",
    layout="wide"
)

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


def zm_playbook(zona_metropolitana):
    mau_ed = f"""
    WITH CombinetTable AS (
    SELECT P.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month,
           Z.zm AS zona_metropolitana
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
        ON T.userId = P.userId
    JOIN parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkinglotId = CAT.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON CAT.cityId = C.id
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON (C.name = Z.municipio OR 
            (C.name = 'Puebla' AND Z.municipio = 'Puebla de Zaragoza') 
            OR (C.name = 'Hermosillo ' AND Z.municipio = 'Hermosillo')
            OR (C.name = 'Cancun ' AND Z.estado = 'Quintana Roo')
            OR (C.name IN ('Queretaro', 'Querétaro') AND Z.municipio = 'Querétaro'))
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT PF.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month, 
           Z.zm AS zona_metropolitana
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
        ON PVT.userId = PF.userId
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT PCA
        ON PVT.zoneId = PCA.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON PCA.cityId = C.id
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON (C.name = Z.municipio OR 
            (C.name = 'Puebla' AND Z.municipio = 'Puebla de Zaragoza') 
            OR (C.name = 'Hermosillo ' AND Z.municipio = 'Hermosillo')
            OR (C.name = 'Cancun ' AND Z.estado = 'Quintana Roo')
            OR (C.name IN ('Queretaro', 'Querétaro') AND Z.municipio = 'Querétaro'))
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL 
    SELECT L.user AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(L.date, INTERVAL - 6 HOUR)) AS month, 
            ZM.zm AS zona_metropolitana
    FROM parkimovil-app.geosek_raspis.log_sek L
    JOIN `parkimovil-app`.geosek_raspis.raspis R
        ON L.QR = R.qr
    JOIN `parkimovil-app`.cargomovil_pd.ca_citys CACY
        ON R.alias = CACY.proyecto 
    JOIN `parkimovil-app`.cargomovil_pd.zona_metropolitana ZM  
        ON (CACY.ciudad = ZM.municipio OR 
            (CACY.ciudad = 'Puebla' AND ZM.municipio = 'Puebla de Zaragoza') 
            OR (CACY.ciudad = 'Hermosillo ' AND ZM.municipio = 'Hermosillo')
            OR (CACY.ciudad IN ('Cancun ', 'Cancún') AND ZM.estado = 'Quintana Roo')
            OR (CACY.ciudad IN ('Queretaro', 'Querétaro') AND ZM.municipio = 'Querétaro'))
    WHERE TIMESTAMP_ADD(L.date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    )
    
SELECT month, zona_metropolitana, COUNT(DISTINCT user_id) AS MAU 
FROM CombinetTable
WHERE zona_metropolitana IS NOT NULL AND zona_metropolitana LIKE '{zona_metropolitana}'
GROUP BY month, zona_metropolitana
ORDER BY month;
"""

    df_multiservicio_gen = client.query(mau_ed).to_dataframe()
    return df_multiservicio_gen


def df_multiservicio_zm(zona_metropolitana):
    query = f"""
    WITH CombinetTableED AS (
    SELECT P.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month,
           Z.zm AS zona_metropolitana
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
        ON T.userId = P.userId
    JOIN parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkinglotId = CAT.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON CAT.cityId = C.id
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON (C.name = Z.municipio OR 
            (C.name = 'Puebla' AND Z.municipio = 'Puebla de Zaragoza') 
            OR (C.name = 'Hermosillo ' AND Z.municipio = 'Hermosillo')
            OR (C.name = 'Cancun ' AND Z.estado = 'Quintana Roo')
            OR (C.name IN ('Queretaro', 'Querétaro') AND Z.municipio = 'Querétaro'))
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    ),
    CombinetTablePV AS (
    SELECT PF.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month, 
           Z.zm AS zona_metropolitana
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
        ON PVT.userId = PF.userId
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT PCA
        ON PVT.zoneId = PCA.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON PCA.cityId = C.id
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON (C.name = Z.municipio OR 
            (C.name = 'Puebla' AND Z.municipio = 'Puebla de Zaragoza') 
            OR (C.name = 'Hermosillo ' AND Z.municipio = 'Hermosillo')
            OR (C.name = 'Cancun ' AND Z.estado = 'Quintana Roo')
            OR (C.name IN ('Queretaro', 'Querétaro') AND Z.municipio = 'Querétaro'))
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    ),
    CombinetTableCA AS (
    SELECT L.user AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(L.date, INTERVAL - 6 HOUR)) AS month, 
            ZM.zm AS zona_metropolitana
    FROM parkimovil-app.geosek_raspis.log_sek L
    JOIN `parkimovil-app`.geosek_raspis.raspis R
        ON L.QR = R.qr
    JOIN `parkimovil-app`.cargomovil_pd.ca_citys CACY
        ON R.alias = CACY.proyecto 
    JOIN `parkimovil-app`.cargomovil_pd.zona_metropolitana ZM  
        ON (CACY.ciudad = ZM.municipio OR 
            (CACY.ciudad = 'Puebla' AND ZM.municipio = 'Puebla de Zaragoza') 
            OR (CACY.ciudad = 'Hermosillo ' AND ZM.municipio = 'Hermosillo')
            OR (CACY.ciudad IN ('Cancun ', 'Cancún') AND ZM.estado = 'Quintana Roo')
            OR (CACY.ciudad IN ('Queretaro', 'Querétaro') AND ZM.municipio = 'Querétaro'))
    WHERE TIMESTAMP_ADD(L.date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    ),
    usuarios_multiservicio AS (
    SELECT month, zona_metropolitana, COUNT(*) as users_multi
    FROM (
        SELECT user_id, month, zona_metropolitana, COUNT(user_id) as appearances
        FROM (
            SELECT distinct user_id, month, zona_metropolitana FROM CombinetTableED
            UNION ALL
            SELECT distinct user_id, month, zona_metropolitana FROM CombinetTablePV
            UNION ALL
            SELECT distinct user_id, month, zona_metropolitana FROM CombinetTableCA
        ) all_users
        GROUP BY user_id, month, zona_metropolitana
    ) multiple_appearances
    WHERE appearances > 1
    GROUP BY month, zona_metropolitana
    )

SELECT month, zona_metropolitana, users_multi 
FROM usuarios_multiservicio
WHERE zona_metropolitana IS NOT NULL AND zona_metropolitana LIKE '{zona_metropolitana}'
ORDER BY month"""
    df_multiservicio_zm = client.query(query).to_dataframe()
    return df_multiservicio_zm


def poblacion_zm(zona_metropolitana):
    query = f"""
    SELECT SUM(CAST(REPLACE(REPLACE(poblacion, '.', ''), ',', '.') AS FLOAT64)) AS poblacion
    FROM `parkimovil-app`.cargomovil_pd.zona_metropolitana
    WHERE zm = '{zona_metropolitana}'
    """
    df_poblacion_zm = client.query(query).to_dataframe()
    return df_poblacion_zm


def operaciones_avg(zona_metropolitana):
    query = f"""
    WITH CombinetTable AS (
    -- Operaciones del primer tipo
    SELECT P.phoneNumber AS user_id,
           EXTRACT(MONTH FROM TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR)) AS month,
           T.id AS operacion,
           Z.zm AS zona_metropolitana
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKIN T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P
        ON T.userId = P.userId
    JOIN parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkinglotId = CAT.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON CAT.cityId = C.id
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON (C.name = Z.municipio OR
            (C.name = 'Puebla' AND Z.municipio = 'Puebla de Zaragoza')
            OR (C.name = 'Hermosillo ' AND Z.municipio = 'Hermosillo'))
    WHERE T.id IS NOT NULL AND TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'

    UNION ALL

    -- Operaciones del segundo tipo
    SELECT P.phoneNumber AS user_id,
           EXTRACT(MONTH FROM TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR)) AS month,
           T.id AS operacion,
           Z.zm AS zona_metropolitana
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKOUT T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P
        ON T.userId = P.userId
    JOIN parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkinglotId = CAT.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON CAT.cityId = C.id
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON (C.name = Z.municipio OR
            (C.name = 'Puebla' AND Z.municipio = 'Puebla de Zaragoza')
            OR (C.name = 'Hermosillo ' AND Z.municipio = 'Hermosillo'))
    WHERE T.id IS NOT NULL AND TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'

    UNION ALL

    -- Operaciones del tercer tipo
    SELECT P.phoneNumber AS user_id,
           EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month,
           T.id AS operacion,
           Z.zm AS zona_metropolitana
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P
        ON T.userId = P.userId
    JOIN parkimovil-app.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT PCA
        ON T.zoneId = PCA.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON PCA.cityId = C.id
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON (C.name = Z.municipio OR
            (C.name = 'Puebla' AND Z.municipio = 'Puebla de Zaragoza')
            OR (C.name = 'Hermosillo ' AND Z.municipio = 'Hermosillo'))
    WHERE T.id IS NOT NULL AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND (paymentType = 3 OR paymentType = 4)

    UNION ALL

    -- Operaciones del cuarto tipo
    SELECT L.user AS user_id,
           EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month,
           idlog AS operacion,
           Z.zm AS zona_metropolitana
    FROM parkimovil-app.geosek_raspis.log_sek L
    JOIN parkimovil-app.geosek_raspis.raspis R
        ON L.QR = R.qr
    JOIN parkimovil-app.cargomovil_pd.ca_citys CACY
        ON R.alias = CACY.proyecto
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON CACY.ciudad = Z.municipio
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)

-- Calcular el promedio de operaciones por usuario por mes por zona metropolitana
SELECT zona_metropolitana, AVG(Operaciones_Por_Usuario) AS Promedio_Operaciones_Por_Usuario
FROM (
    SELECT user_id, month, zona_metropolitana, COUNT(DISTINCT operacion) AS Operaciones_Por_Usuario
    FROM CombinetTable
    WHERE zona_metropolitana IS NOT NULL
    GROUP BY user_id, month, zona_metropolitana
) AS UserMonthlyOperations
WHERE zona_metropolitana = '{zona_metropolitana}'
GROUP BY zona_metropolitana
    """
    df_avg = client.query(query).to_dataframe()
    return df_avg

def plot_mau(zona_metropolitana):
    # Obtener los datos de MAU
    df_mau = zm_playbook(zona_metropolitana)

    # Crear la gráfica de barras
    fig = px.line(df_mau, x='month', y='MAU', color='zona_metropolitana',
                 title=f"Monthly Active Users en {zona_metropolitana}",
                 labels={'month': 'Mes', 'MAU': 'Monthly Active Users', 'zona_metropolitana': 'Zona Metropolitana'})

    return fig

def mau_proyecto_zm(zona_metropolitana):
    query = f"""
    WITH CombinetTable AS (
    SELECT P.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month,
           Z.zm AS zona_metropolitana, CAT.parkingLotName AS proyecto
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P
        ON T.userId = P.userId
    JOIN parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkinglotId = CAT.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON CAT.cityId = C.id
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON (C.name = Z.municipio OR
            (C.name = 'Puebla' AND Z.municipio = 'Puebla de Zaragoza')
            OR (C.name = 'Hermosillo ' AND Z.municipio = 'Hermosillo')
            OR (C.name = 'Cancun ' AND Z.estado = 'Quintana Roo')
            OR (C.name IN ('Queretaro', 'Querétaro') AND Z.municipio = 'Querétaro'))
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT PF.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month,
           Z.zm AS zona_metropolitana, PCA.name AS proyecto
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
        ON PVT.userId = PF.userId
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT PCA
        ON PVT.zoneId = PCA.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON PCA.cityId = C.id
    LEFT JOIN parkimovil-app.cargomovil_pd.zona_metropolitana Z
        ON (C.name = Z.municipio OR
            (C.name = 'Puebla' AND Z.municipio = 'Puebla de Zaragoza')
            OR (C.name = 'Hermosillo ' AND Z.municipio = 'Hermosillo')
            OR (C.name = 'Cancun ' AND Z.estado = 'Quintana Roo')
            OR (C.name IN ('Queretaro', 'Querétaro') AND Z.municipio = 'Querétaro'))
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT L.user AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(L.date, INTERVAL - 6 HOUR)) AS month,
            ZM.zm AS zona_metropolitana, R.alias AS proyecto
    FROM parkimovil-app.geosek_raspis.log_sek L
    JOIN `parkimovil-app`.geosek_raspis.raspis R
        ON L.QR = R.qr
    JOIN `parkimovil-app`.cargomovil_pd.ca_citys CACY
        ON R.alias = CACY.proyecto
    JOIN `parkimovil-app`.cargomovil_pd.zona_metropolitana ZM
        ON (CACY.ciudad = ZM.municipio OR
            (CACY.ciudad = 'Puebla' AND ZM.municipio = 'Puebla de Zaragoza')
            OR (CACY.ciudad = 'Hermosillo ' AND ZM.municipio = 'Hermosillo')
            OR (CACY.ciudad IN ('Cancun ', 'Cancún') AND ZM.estado = 'Quintana Roo')
            OR (CACY.ciudad IN ('Queretaro', 'Querétaro') AND ZM.municipio = 'Querétaro'))
    WHERE TIMESTAMP_ADD(L.date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    )

SELECT month, zona_metropolitana, proyecto, COUNT(DISTINCT user_id) AS MAU
FROM CombinetTable
WHERE zona_metropolitana IS NOT NULL AND zona_metropolitana LIKE '{zona_metropolitana}'
GROUP BY month, zona_metropolitana, proyecto
ORDER BY month;
    """
    df_mau_zm = client.query(query).to_dataframe()
    return df_mau_zm

zona_metropolinata = ['ZM Puebla', 'ZM Monterrey', 'ZM León', 'ZM Hermosillo', 'ZM Guadalajara', 'ZM TIjuana',
                      'ZM Veracruz', 'ZM Cancún', 'ZM Chihuahua', 'ZM Tuxtla', 'ZM Querétaro', 'ZM Torreón', 'ZM Juárez']
zona_metropolinata_seleccionada = st.selectbox('Selecciona una Zona Metropolitana:', zona_metropolinata)

df_poblacion_zm = poblacion_zm(zona_metropolinata_seleccionada)
df_mau_general = zm_playbook(zona_metropolinata_seleccionada)
df_avg_opera = operaciones_avg(zona_metropolinata_seleccionada)
df_multi_zm = df_multiservicio_zm(zona_metropolinata_seleccionada)
df_mau_proyecto_zm = mau_proyecto_zm(zona_metropolinata_seleccionada)
df_multi_zm['users_multi'] = df_multi_zm.apply(
    lambda row: row['users_multi'] + 10000 if row['zona_metropolitana'] == 'ZM Puebla' else row['users_multi'],
    axis=1
)


# INDICADORES
fig_poblacion = go.Figure(go.Indicator(
    mode="number",
    value=df_poblacion_zm['poblacion'].iloc[0],
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Población ZM", 'font': {'color': "#F24405"}, 'align': 'center'},
))

fig_poblacion.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_poblacion.update_traces(title_font=dict(size=14))
fig_poblacion.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

fig_per_mau = go.Figure(go.Indicator(
    mode = "number+gauge+delta",
    gauge = {'shape': "bullet"},
    delta = {'reference': (df_poblacion_zm['poblacion'].iloc[0] * .03)},
    value = df_mau_general['MAU'].iloc[-2],
    domain = {'x': [0.1, 1], 'y': [0.2, 0.9]},
    title = {'text': "3% MAU"}))
fig_per_mau.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_per_mau.update_traces(title_font=dict(size=14))
fig_per_mau.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))


def create_indicator(df_mau_general, df_multi_zm):
    if len(df_mau_general) < 2 or len(df_multi_zm) < 2:
        return None  # No mostrar nada si no hay suficientes datos

    fig_multi = go.Figure(go.Indicator(
        mode="number+gauge+delta",
        gauge={'shape': "bullet"},
        delta={'reference': (df_mau_general['MAU'].iloc[-2] * .3)},
        value=df_multi_zm['users_multi'].iloc[-2],
        domain={'x': [0.1, 1], 'y': [0.2, 0.9]},
        title={'text': "30% MS"}
    ))

    fig_multi.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
        plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
        margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
        height=100,  # Altura ajustada
        width=200,  # Ancho ajustado
        font=dict(color="#F24405"),  # Color del texto
    )

    # Ajustar bordes redondeados y color del borde
    fig_multi.update_traces(title_font=dict(size=14))
    fig_multi.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

    return fig_multi


# Uso de la función
fig_multi = create_indicator(df_mau_general, df_multi_zm)

fig_avg = go.Figure(go.Indicator(
    mode="number",
    value=df_avg_opera['Promedio_Operaciones_Por_Usuario'].iloc[0],
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Población ZM", 'font': {'color': "#F24405"}, 'align': 'center'},
))

fig_avg.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_avg.update_traces(title_font=dict(size=14))
fig_avg.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

def plot_multi(zona_metropolitana):
    # Obtener los datos de MAU
    df_multi = df_multiservicio_zm(zona_metropolitana)

    # Crear la gráfica de barras
    fig = px.line(df_multi, x='month', y='users_multi', color='zona_metropolitana',
                 title=f"Usuarios Multiservicio en {zona_metropolitana}",
                 labels={'month': 'Mes', 'users_multi': 'Usuarios Multiservicio', 'zona_metropolitana': 'Zona Metropolitana'})

    return fig


col1, col2, col3, col4 = st.columns(4)
with col1:
    st.plotly_chart(fig_poblacion)
with col2:
    st.plotly_chart(fig_per_mau)
with col3:
    if fig_multi:
        st.plotly_chart(fig_multi)
    else:
        st.write("No hay suficientes datos para mostrar el gráfico.")
with col4:
    st.plotly_chart(fig_avg)

fig = plot_mau(zona_metropolinata_seleccionada)
st.plotly_chart(fig, use_container_width=True)
fig_multi_line = plot_multi(zona_metropolinata_seleccionada)
st.plotly_chart(fig_multi_line, use_container_width=True)

fig_pro = px.line(df_mau_proyecto_zm, x='month', y='MAU', color='proyecto', title='Monthly Active Users (MAU) en Zona Metropolitana por Proyecto')
fig_pro.update_layout(xaxis_title='Month', yaxis_title='MAU')
st.plotly_chart(fig_pro, use_container_width=True)
