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
    page_title="Kigo - Stickiness",
    layout="wide"
)

st.sidebar.success("PLAYBOOK.")

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# OPERACIONES GENERALES
operaciones_gen = """
WITH CombinetTable AS (
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR)) AS month, id AS operacion
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKIN
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'
    UNION ALL
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR)) AS month, id AS operacion
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKOUT
    WHERE  id IS NOT NULL AND TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'
    UNION ALL
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month, id AS operacion
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION 
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND (paymentType = 3 OR paymentType = 4)
    UNION ALL
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month, idlog AS operacion
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)
-- APP
SELECT month, COUNT(DISTINCT operacion) AS Operaciones
FROM CombinetTable
GROUP BY month 
ORDER BY month;
"""

# TRANSACCIONES GENERALES
transacciones_gen = """
-- APP
WITH CombinetTable AS (
    SELECT T.transactionId AS transacciones, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND T.total != 0 AND T.total IS NOT NULL AND T.qrCode LIKE 'E%'
    UNION ALL
    SELECT PVT.id AS transacciones, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND (PVT.paymentType = 3 OR PVT.paymentType = 4) AND PVT.amount != 0 AND PVT.amount IS NOT NULL
    )

    SELECT month, COUNT(DISTINCT transacciones) AS Transacciones
    FROM CombinetTable
    GROUP BY month
    ORDER BY month;
"""

df_transacciones_general = client.query(transacciones_gen).to_dataframe()
trans_last_month = df_transacciones_general['Transacciones'].iloc[-3]
trans_actural = df_transacciones_general['Transacciones'].iloc[-2]


df_operaciones_general = client.query(operaciones_gen).to_dataframe()
df_operaciones_general = df_operaciones_general.iloc[0:6]
op_last_month = df_operaciones_general['Operaciones'].iloc[-2]
op_actual = df_operaciones_general['Operaciones'].iloc[-1]




# PROYECCION OPERACIONES
model = ARIMA(df_operaciones_general['Operaciones'].iloc[0:-1], order=(1, 1, 1))  # Ajustar el orden del modelo según sea necesario
model_fit = model.fit()
forecast = model_fit.forecast(steps=1)

prediccion = forecast.iloc[0].astype('int64')

pred_ope = go.Figure()

# Añadir barras al gráfico
pred_ope.add_trace(go.Bar(
    x=df_operaciones_general['month'],
    y=df_operaciones_general['Operaciones'],
    name='Operaciones'
))

# Resaltar el último valor con un color diferente
ultimo_valor = prediccion
pred_ope.add_trace(go.Bar(
    x=[df_operaciones_general.iloc[-1]['month']],
    y=[ultimo_valor],
    marker=dict(color='red'),  # Cambiar el color del último valor
    name='Proyección',
    marker_color='#F24405'
))

# Actualizar diseño del gráfico
pred_ope.update_layout(
    title='Proyeccion de Operaciones para el Mes en Curso',
    xaxis_title='Mes',
    yaxis_title='Operaciones'
)

# PROYECCIÓN TRANSACCIONES
model_tran = ARIMA(df_transacciones_general['Transacciones'].iloc[0:4], order=(1, 1, 1))
model_fit = model_tran.fit()
forecast = model_fit.forecast(steps=1)

prediccion = forecast.iloc[0].astype('int64')

tran_pred = go.Figure()

tran_pred.add_trace(go.Bar(
    x=df_transacciones_general['month'],
    y=df_transacciones_general['Transacciones'],
    name='Transacciones'
))

ultimo_valor = prediccion
tran_pred.add_trace(go.Bar(
    x=[df_transacciones_general.iloc[-1]['month']],
    y=[ultimo_valor],
    marker=dict(color='#F24405'),
    name='Proyección'
))

tran_pred.update_layout(
    title='Proyecciones de Transacciones para el Mes en Curso',
    xaxis_title='Mes',
    yaxis_title='Transacciones'
)


st.plotly_chart(pred_ope)
st.plotly_chart(tran_pred)