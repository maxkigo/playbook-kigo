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
    page_title="Kigo - Playbool Awareness",
    layout="wide"
)


# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

mau_general = """
    WITH CombinetTable AS (
    SELECT P.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
        ON T.userId = P.userId
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT PF.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
        ON PVT.userId = PF.userId
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL 
    SELECT user AS user_id, EXTRACT(MONTH FROM date) AS mes
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE TIMESTAMP(date) >= '2024-01-01 00:00:00'
    )

    SELECT mes, COUNT(DISTINCT user_id) AS MAU 
    FROM CombinetTable
    GROUP BY mes
    ORDER BY mes;
    """

df_mau_general = client.query(mau_general).to_dataframe()
mau_last_month = df_mau_general['MAU'].iloc[-3]
mau_actual = df_mau_general['MAU'].iloc[-2]

def mau_servicio(servicio):
    if servicio == 'Todos los Servicios':
        mau = mau_general
    elif servicio == 'ED':
        mau = """
        SELECT COUNT(DISTINCT P.phoneNumber) AS MAU, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
        ON T.userId = P.userId
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    GROUP BY EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR))
    ORDER BY mes
        """
    elif servicio == 'PV':
        mau = """
        SELECT COUNT(DISTINCT PF.phoneNumber) AS MAU, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
        ON PVT.userId = PF.userId
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    GROUP BY EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR))
    ORDER BY mes
        """
    elif servicio == 'CA':
        mau = """
        SELECT COUNT(DISTINCT user) AS MAU, EXTRACT(MONTH FROM date) AS mes
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE TIMESTAMP(date) >= '2024-01-01 00:00:00'
    GROUP BY EXTRACT(MONTH FROM date)
    ORDER BY mes
        """

    df_mau_general = client.query(mau).to_dataframe()

    return df_mau_general


servicios = ['Todos los Servicios', 'PV', 'ED', 'CA']
servicio_seleccionado = st.selectbox('Selecciona un servicio:', servicios)

df_mau_general = mau_servicio(servicio_seleccionado)
mau_last_month = df_mau_general['MAU'].iloc[-3]
mau_actual = df_mau_general['MAU'].iloc[-2]

# PROYECCION MAU
model = ARIMA(df_mau_general['MAU'].iloc[0:-1], order=(3, 2, 3))
model_fit = model.fit()
forecast = model_fit.forecast(steps=1)

prediccion = forecast.iloc[0].astype('int64')

fig = go.Figure()

# Añadir barras al gráfico
fig.add_trace(go.Bar(
    x=df_mau_general['mes'],
    y=df_mau_general['MAU'],
    name='MAU'
))

# Resaltar el último valor con un color diferente
ultimo_valor = prediccion
fig.add_trace(go.Bar(
    x=[df_mau_general.iloc[-1]['mes']],
    y=[ultimo_valor],
    marker=dict(color='red'),  # Cambiar el color del último valor
    name='Proyección',
    marker_color='#F24405'
))

# Actualizar diseño del gráfico
fig.update_layout(
    title='Proyección de MAU para el Mes en Curso',
    xaxis_title='mes',
    yaxis_title='MAU'
)

st.plotly_chart(fig, use_container_width=True)