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
    SELECT S.phoneNumber AS user_id, T.transactionId AS Operacion, 
    EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month, 
    EXTRACT(YEAR FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS year
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTablePV AS (
    SELECT S.phoneNumber AS user_id, T.transactionId AS Operacion, 
    EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month,
    EXTRACT(YEAR FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS year
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTableCA AS (
    SELECT user AS user_id, idlog AS operacion, 
    EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month,
    EXTRACT(YEAR FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS year
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)

SELECT year, month, COUNT(*) as users_multi
FROM (
    SELECT user_id, year, month, COUNT(*) as appearances
    FROM (
        SELECT distinct user_id, month, year FROM usuariosTableED
        UNION ALL
        SELECT distinct user_id, month, year FROM usuariosTablePV
        UNION ALL
        SELECT distinct user_id, month, year FROM usuariosTableCA
    ) all_users
    GROUP BY user_id, month, year
) multiple_appearances
WHERE appearances > 1
GROUP BY year, month
ORDER BY year, month;       
"""

df_multiservicio_gen = client.query(usuarios_multiservicio).to_dataframe()

# Consulta para usuarios multiproyecto
multi_proyecto = """
WITH combined AS (
    SELECT CAT.parkingLotName AS proyecto, S.phoneNumber AS user_id, 
    EXTRACT(YEAR FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS year, 
    EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkingLotId = CAT.id
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT ZCA.name AS proyecto, S.phoneNumber AS user_id, 
    EXTRACT(YEAR FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS year,
    EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT ZCA
        ON T.zoneId = ZCA.id
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT R.alias AS proyecto, user AS user_id, 
    EXTRACT(YEAR FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS year,
    EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
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
df_multiservicio_gen['mes'] = df_multiservicio_gen.apply(lambda row: f"{row['year']}-{row['month']:02d}", axis=1)
fig_multiservicio.add_trace(go.Bar(
    x=df_multiservicio_gen['mes'],
    y=df_multiservicio_gen['users_multi'],
    name='Usuarios Multiservicio',
    marker_color='#EEA31B'
))
fig_multiservicio.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
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
fig_multiproyecto.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
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
    SELECT DISTINCT user_id, COUNT(DISTINCT proyecto) as num_proyectos
    FROM combined
    GROUP BY user_id
) proyecto_count
GROUP BY num_proyectos
ORDER BY num_proyectos;
"""

df_multiproyecto_funnel = client.query(multi_proyecto_funnel).to_dataframe()

# Total de Usuarios
usuarios_total_funnel = """
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

SELECT COUNT(DISTINCT user_id) as users
FROM (
        SELECT distinct user_id FROM usuariosTableED
        UNION ALL
        SELECT distinct user_id FROM usuariosTablePV
        UNION ALL
        SELECT distinct user_id FROM usuariosTableCA
    ) all_users
"""

total_usuarios = client.query(usuarios_total_funnel).to_dataframe()

# Total de usuarios para el embudo multiservicio
total_usuarios_multiservicio = total_usuarios['users'].sum()

# Agregar nivel 100% al DataFrame de usuarios multiservicio
df_multiservicio_funnel = pd.concat([pd.DataFrame({'num_servicios': [0], 'users': [total_usuarios_multiservicio]}),
                                     df_multiservicio_funnel])

# Total de usuarios para el embudo multiproyecto
total_usuarios_multiproyecto = total_usuarios['users'].sum()

# Agregar nivel 100% al DataFrame de usuarios multiproyecto
df_multiproyecto_funnel = pd.concat([pd.DataFrame({'num_proyectos': [0], 'users': [total_usuarios_multiproyecto]}),
                                     df_multiproyecto_funnel])

#-------------------- Comparativa uniservicio vs multiservicio

churn_multy = """
    WITH usuariosTableED AS (
    SELECT S.phoneNumber AS user_id, EXTRACT(YEAR FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS year, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS mes
    FROM `parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS` T
    JOIN `parkimovil-app.cargomovil_pd.SEC_USER_PROFILE` S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTablePV AS (
    SELECT S.phoneNumber AS user_id, EXTRACT(YEAR FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS year, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes
    FROM `parkimovil-app.cargomovil_pd.PKM_TRANSACTION` T
    JOIN `parkimovil-app.cargomovil_pd.SEC_USER_PROFILE` S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTableCA AS (
    SELECT user AS user_id, EXTRACT(YEAR FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS year, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes
    FROM `parkimovil-app.geosek_raspis.log_sek`
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuarios_multiservicio AS (
    SELECT user_id, year, mes, COUNT(DISTINCT servicio) AS num_servicios
    FROM (
        SELECT DISTINCT user_id, year, mes, 'ED' AS servicio FROM usuariosTableED
        UNION ALL
        SELECT DISTINCT user_id, year, mes, 'PV' AS servicio FROM usuariosTablePV
        UNION ALL
        SELECT DISTINCT user_id, year, mes, 'CA' AS servicio FROM usuariosTableCA
    ) all_users
    GROUP BY user_id, year, mes
    HAVING num_servicios > 1
),
usuarios_uniservicio AS (
    SELECT user_id, year, mes, COUNT(DISTINCT servicio) AS num_servicios
    FROM (
        SELECT DISTINCT user_id, year, mes, 'ED' AS servicio FROM usuariosTableED
        UNION ALL
        SELECT DISTINCT user_id, year, mes, 'PV' AS servicio FROM usuariosTablePV
        UNION ALL
        SELECT DISTINCT user_id, year, mes, 'CA' AS servicio FROM usuariosTableCA
    ) all_users
    GROUP BY user_id, year, mes
    HAVING num_servicios = 1
),
usuarios_churn AS (
    SELECT a.user_id, a.year, a.mes AS mes_anterior
    FROM usuarios_multiservicio a
    LEFT JOIN usuarios_multiservicio b
    ON a.user_id = b.user_id AND a.year = b.year AND a.mes = b.mes + 1
    LEFT JOIN usuarios_uniservicio c
    ON a.user_id = c.user_id AND a.year = c.year AND a.mes = c.mes + 1
    WHERE b.user_id IS NULL AND c.user_id IS NOT NULL
)
SELECT year, mes_anterior AS mes, COUNT(DISTINCT user_id) AS usuarios_churn
FROM usuarios_churn
GROUP BY year, mes_anterior
ORDER BY year, mes_anterior
    """

df_churn_multy = client.query(churn_multy).to_dataframe()

multiservicio_usuarios_nuevos = """
WITH usuariosTableED AS (
    SELECT S.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTablePV AS (
    SELECT S.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuariosTableCA AS (
    SELECT user AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
),
usuarios_multiservicio AS (
    SELECT user_id, mes, COUNT(DISTINCT servicio) as num_servicios
    FROM (
        SELECT distinct user_id, mes, 'ED' as servicio FROM usuariosTableED
        UNION ALL
        SELECT distinct user_id, mes, 'PV' as servicio FROM usuariosTablePV
        UNION ALL
        SELECT distinct user_id, mes, 'CA' as servicio FROM usuariosTableCA
    ) all_users
    GROUP BY user_id, mes
    HAVING num_servicios > 1
),
usuarios_nuevos AS (
    SELECT b.user_id, b.mes AS mes_actual
    FROM usuarios_multiservicio b
    LEFT JOIN usuarios_multiservicio a
    ON b.user_id = a.user_id AND b.mes = a.mes + 1
    WHERE a.user_id IS NULL
)
SELECT mes_actual AS mes, COUNT(DISTINCT user_id) AS nuevos_usuarios
FROM usuarios_nuevos
GROUP BY mes_actual
ORDER BY mes_actual
"""

df_multiservicio_user_nuevos = client.query(multiservicio_usuarios_nuevos).to_dataframe()

multi_proyecto_churn = """
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
),
usuarios_uniservicio AS (
    SELECT user_id, year, month
    FROM combined
    GROUP BY user_id, year, month
    HAVING COUNT(DISTINCT proyecto) = 1
),
usuarios_churn AS (
    SELECT a.user_id, a.year, a.month AS mes_anterior
    FROM usuarios_multiservicio a
    LEFT JOIN usuarios_multiservicio b
    ON a.user_id = b.user_id AND a.year = b.year AND a.month = b.month + 1
    LEFT JOIN usuarios_uniservicio c
    ON a.user_id = c.user_id AND a.year = c.year AND a.month = c.month + 1
    WHERE b.user_id IS NULL AND c.user_id IS NOT NULL
)
SELECT year, mes_anterior AS mes, COUNT(DISTINCT user_id) AS usuarios_churn
FROM usuarios_churn
GROUP BY year, mes_anterior
ORDER BY year, mes_anterior
"""

df_churn_multyproyecto = client.query(multi_proyecto_churn).to_dataframe()

multi_proyecto_user_new = """
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
),
usuarios_nuevos AS (
    SELECT b.user_id, b.year, b.month AS mes_actual
    FROM usuarios_multiservicio b
    LEFT JOIN usuarios_multiservicio a
    ON b.user_id = a.user_id AND b.year = a.year AND b.month = a.month + 1
    WHERE a.user_id IS NULL
)
SELECT year, mes_actual AS mes, COUNT(DISTINCT user_id) AS nuevos_usuarios
FROM usuarios_nuevos
GROUP BY year, mes_actual
ORDER BY year, mes_actual
"""

df_multiproyecto_user_nuevos = client.query(multi_proyecto_user_new).to_dataframe()

#-----------------------------------FIN----------------------------------------#

# Gráfico de embudo para usuarios multiservicio
fig_funnel_multiservicio = go.Figure(go.Funnel(
    y = df_multiservicio_funnel['num_servicios'],
    x = df_multiservicio_funnel['users'],
    marker = {"color": ["#030140", "#F24405", "#4F70B7", "#F88201", "#EEA31B"]},
    textinfo = "value+percent initial"
))
fig_funnel_multiservicio.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
fig_funnel_multiservicio.update_layout(
    title='Funnel de Usuarios Multiservicio',
    yaxis_title='Número de Servicios',
    xaxis_title='Número de Usuarios'
)

# Gráfico de embudo para usuarios multiproyecto
fig_funnel_multiproyecto = go.Figure(go.Funnel(
    y = df_multiproyecto_funnel['num_proyectos'],
    x = df_multiproyecto_funnel['users'],
    marker = {"color": ["#030140", "#F24405", "#4F70B7", "#F88201", "#EEA31B", "#E31A1A", "#3598DB", "#FFD700", "#FFBC4D",
    "#90CAF9", "#C5E17A", "#673AB7", "#D65000", "#FFAB91",
    "#C628AA", "#AB47BC", "#FFEB3B", "#7CB1EB", "#5C6BC0",
    "#DC00EE", "#FF6384", "#FF9800", "#9E9E9E", "#607D8B",
    "#B4DBE2", "#4DD0E1", "#F0F8FF", "#E6EEFA", "#FFF9C4"]},
    textinfo = "value+percent initial"
))
fig_funnel_multiproyecto.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
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

df_multiservicio_user_nuevos = df_multiservicio_user_nuevos[df_multiservicio_user_nuevos['mes'] != 1]

fig_churn_serv = go.Figure()
fig_churn_serv.add_trace(go.Bar(
    x = df_churn_multy['mes'],
    y = df_churn_multy['usuarios_churn'],
    name = 'Churn de Usuarios Multiservicio',
    marker_color = '#030140'
))
fig_churn_serv.add_trace(go.Bar(
    x = df_multiservicio_user_nuevos['mes'],
    y = df_multiservicio_user_nuevos['nuevos_usuarios'],
    name = 'Nuevos Usuarios Multiservicio',
    marker_color = '#4F70B7'
))
fig_churn_serv.update_layout(
    title='Churn vs Nuevos Usuarios - Multiservicio',
    yaxis_title='Usuarios',
    xaxis_title='Mes'
)
fig_churn_serv.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )

df_multiproyecto_user_nuevos = df_multiproyecto_user_nuevos[df_multiproyecto_user_nuevos['mes'] != 1]

fig_churn_proy = go.Figure()
fig_churn_proy.add_trace(go.Bar(
    x=df_churn_multyproyecto['mes'],
    y=df_churn_multyproyecto['usuarios_churn'],
    name = 'Churn de Usuarios Multiproyecto',
    marker_color = '#F24405'
))
fig_churn_proy.add_trace(go.Bar(
    x=df_multiproyecto_user_nuevos['mes'],
    y=df_multiproyecto_user_nuevos['nuevos_usuarios'],
    name = 'Nuevos Usuarios Multiproyecto',
    marker_color = '#F88201'
))
fig_churn_proy.update_layout(
    title='Churn vs Nuevos Usuarios - Multiproyecto',
    yaxis_title='Usuarios',
    xaxis_title='Mes'
)
fig_churn_proy.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )

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


col11, col12 = st.columns(2)
with col11:
    st.plotly_chart(fig_churn_serv)
with col12:
    st.plotly_chart(fig_churn_proy)