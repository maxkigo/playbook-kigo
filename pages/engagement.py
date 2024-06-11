import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.oauth2 import service_account

# Configuración de la página
st.set_page_config(
    page_title="Kigo - Engagement",
    layout="wide"
)

col7, col8, col9, col10 = st.columns(4)

with col7:
    st.write()

with col8:
    st.image('https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/logos.png')

with col9:
    st.title('Kigo Engagement')

with col10:
    st.write()

st.sidebar.success("PLAYBOOK.")

# Crear cliente de API.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Consulta para usuarios multiservicio
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

# Consulta para usuarios multiproyecto
multi_proyecto = """
WITH combined AS (
    SELECT CAT.parkingLotName AS proyecto, S.phoneNumber AS user_id, EXTRACT(YEAR FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS year, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkingLotId = CAT.id
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT ZCA.name AS proyecto, S.phoneNumber AS user_id, EXTRACT(YEAR FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS year, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT ZCA
        ON T.zoneId = ZCA.id
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT R.alias AS proyecto, user AS user_id, EXTRACT(YEAR FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS year, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.geosek_raspis.log_sek L
    JOIN `parkimovil-app`.geosek_raspis.raspis R
        ON L.QR = R.qr
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuarios_multiservicio AS (
    SELECT user_id, year, month
    FROM combined
    GROUP BY user_id, year, month
    HAVING COUNT(DISTINCT proyecto) > 1
)
SELECT year, month, COUNT(*) AS usuarios_multiservicio_por_mes
FROM usuarios_multiservicio
GROUP BY year, month
ORDER BY year, month;
"""

df_multiproyecto_gen = client.query(multi_proyecto).to_dataframe()

# Gráfico de usuarios multiservicio
fig_multiservicio = go.Figure()

fig_multiservicio.add_trace(go.Bar(
    x=df_multiservicio_gen['mes'],
    y=df_multiservicio_gen['users_multi'],
    name='Usuarios Multiservicio',
    marker_color='#EEA31B'
))

fig_multiservicio.update_layout(
    title='Usuarios Multiservicio por Mes',
    xaxis_title='Mes',
    yaxis_title='Número de Usuarios',
    barmode='group'
)

# Gráfico de usuarios multiproyecto
fig_multiproyecto = go.Figure()

df_multiproyecto_gen['mes'] = df_multiproyecto_gen.apply(lambda row: f"{row['year']}-{row['month']:02d}", axis=1)

fig_multiproyecto.add_trace(go.Bar(
    x=df_multiproyecto_gen['mes'],
    y=df_multiproyecto_gen['usuarios_multiservicio_por_mes'],
    name='Usuarios Multiproyecto',
    marker_color='#4F70B7'
))

fig_multiproyecto.update_layout(
    title='Usuarios Multiproyecto por Mes',
    xaxis_title='Mes',
    yaxis_title='Número de Usuarios',
    barmode='group'
)

# Funnel de usuarios multiservicio
usuarios_multiservicio_funnel = """
WITH usuariosTableED AS (
    SELECT S.phoneNumber AS user_id, T.transactionId AS Operacion
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTablePV AS (
    SELECT S.phoneNumber AS user_id, T.transactionId AS Operacion
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTableCA AS (
    SELECT user AS user_id, idlog AS operacion
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)

SELECT num_servicios, COUNT(*) as users
FROM (
    SELECT user_id, COUNT(DISTINCT servicio) as num_servicios
    FROM (
        SELECT distinct user_id, 'ED' as servicio FROM usuariosTableED
        UNION ALL
        SELECT distinct user_id, 'PV' as servicio FROM usuariosTablePV
        UNION ALL
        SELECT distinct user_id, 'CA' as servicio FROM usuariosTableCA
    ) all_users
    GROUP BY user_id
) servicio_count
GROUP BY num_servicios
ORDER BY num_servicios;
"""

df_multiservicio_funnel = client.query(usuarios_multiservicio_funnel).to_dataframe()

# Funnel de usuarios multiproyecto
multi_proyecto_funnel = """
WITH combined AS (
    SELECT CAT.parkingLotName AS proyecto, S.phoneNumber AS user_id
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkingLotId = CAT.id
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT ZCA.name AS proyecto, S.phoneNumber AS user_id
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT ZCA
        ON T.zoneId = ZCA.id
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT R.alias AS proyecto, user AS user_id
    FROM parkimovil-app.geosek_raspis.log_sek L
    JOIN `parkimovil-app`.geosek_raspis.raspis R
        ON L.QR = R.qr
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)

SELECT num_proyectos, COUNT(*) as users
FROM (
    SELECT user_id, COUNT(DISTINCT proyecto) as num_proyectos
    FROM combined
    GROUP BY user_id
) proyecto_count
GROUP BY num_proyectos
ORDER BY num_proyectos;
"""

df_multiproyecto_funnel = client.query(multi_proyecto_funnel).to_dataframe()

# Gráfico de embudo para usuarios multiservicio
fig_funnel_multiservicio = go.Figure(go.Funnel(
    y = df_multiservicio_funnel['num_servicios'],
    x = df_multiservicio_funnel['users'],
    marker = {"color": ["#E1BEE7", "#CE93D8", "#BA68C8", "#AB47BC", "#9C27B0"]},
    textinfo = "value+percent initial"
))

fig_funnel_multiservicio.update_layout(
    title='Funnel de Usuarios Multiservicio',
    yaxis_title='Número de Servicios',
    xaxis_title='Número de Usuarios'
)

# Gráfico de embudo para usuarios multiproyecto
fig_funnel_multiproyecto = go.Figure(go.Funnel(
    y = df_multiproyecto_funnel['num_proyectos'],
    x = df_multiproyecto_funnel['users'],
    marker = {"color": ["#BBDEFB", "#90CAF9", "#64B5F6", "#42A5F5", "#2196F3"]},
    textinfo = "value+percent initial"
))

fig_funnel_multiproyecto.update_layout(
    title='Funnel de Usuarios Multiproyecto',
    yaxis_title='Número de Proyectos',
    xaxis_title='Número de Usuarios'
)

# Calcular y mostrar indicadores

# Usuarios multiservicio para el mes pasado y el mes actual
multiservicio_gen_last_month = df_multiservicio_gen['users_multi'].iloc[-2]
multiservicio_gen_actual = df_multiservicio_gen['users_multi'].iloc[-1]

# Usuarios multiproyecto para el mes pasado y el mes actual
multiproyecto_gen_last_month = df_multiproyecto_gen['usuarios_multiservicio_por_mes'].iloc[-2]
multiproyecto_gen_actual = df_multiproyecto_gen['usuarios_multiservicio_por_mes'].iloc[-1]

col1, col2, col3, col4 = st.columns(4)
with col1:
    # Mostrar indicadores
    st.metric(label="Usuarios Multiservicio Mes Pasado", value=round(multiservicio_gen_last_month))
with col2:
    st.metric(label="Usuarios Multiservicio Mes Actual", value=round(multiservicio_gen_actual))
with col3:
    st.metric(label="Usuarios Multiproyecto Mes Pasado", value=round(multiproyecto_gen_last_month))
with col4:
    st.metric(label="Usuarios Multiproyecto Mes Actual", value=round(multiproyecto_gen_actual))

col5, col6 = st.columns(2)
# Mostrar gráficos
with col5:
    st.plotly_chart(fig_multiservicio, use_container_width=True)
with col6:
    st.plotly_chart(fig_multiproyecto, use_container_width=True)

st.plotly_chart(fig_funnel_multiservicio, use_container_width=True)
st.plotly_chart(fig_funnel_multiproyecto, use_container_width=True)


