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
    page_title="Kigo - Engagament",
    layout="wide"
)

st.sidebar.success("PLAYBOOK.")

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

usuarios_multiservicio = """
WITH usuariosTableED AS (
    SELECT S.phoneNumber AS user_id, T.transactionId AS Operacion, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTablePV AS (
    SELECT S.phoneNumber AS user_id, T.transactionId AS Operacion, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTableCA AS (
    SELECT user AS user_id, idlog AS operacion, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)

SELECT mes, COUNT(*) as users_multi
FROM (
    SELECT user_id, mes, COUNT(*) as appearances
    FROM (
        SELECT distinct user_id, mes FROM usuariosTableED
        UNION ALL
        SELECT distinct user_id, mes FROM usuariosTablePV
        UNION ALL
        SELECT distinct user_id, mes FROM usuariosTableCA
    ) all_users
    GROUP BY user_id, mes
) multiple_appearances
WHERE appearances > 1
GROUP BY mes
ORDER BY mes;       
"""

df_multiservicio_gen = client.query(usuarios_multiservicio).to_dataframe()
multiservicio_gen_last_month = df_multiservicio_gen['users_multi'].iloc[-3]
multivervicio_actual = df_multiservicio_gen['users_multi'].iloc[-2]

fig_multiservicio = go.Figure()

fig_multiservicio.add_trace(go.Bar(
    x=df_multiservicio_gen['mes'],
    y=df_multiservicio_gen['users_multi'],
    name='Usuarios Multiservicio'
))

st.plotly_chart(fig_multiservicio, use_container_width=True)