import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración de página
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
    st.title('Kigo Stickiness')

with col4:
    st.write()

st.sidebar.success("PLAYBOOK.")

# Crear cliente de BigQuery
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Función para consultar datos y cachearlos
@st.cache_data(ttl=600)
def fetch_data(query):
    return client.query(query).to_dataframe()

# Consulta para obtener datos de transacciones generales (mensual)
transacciones_gen_mensual_query = """
WITH CombinedTable AS (
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS mes,
           CASE WHEN T.total != 0 AND T.total IS NOT NULL AND T.qrCode LIKE 'E%' THEN 'ED'
           END AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes,
           CASE WHEN PVT.paymentType = 3 OR PVT.paymentType = 4 THEN 'PV'
           END AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)

SELECT mes, tipo_servicio, COUNT(*) AS count_servicio
FROM CombinedTable
GROUP BY mes, tipo_servicio
ORDER BY mes, tipo_servicio;
"""

# Consulta para obtener datos de operaciones generales (mensual)
operaciones_gen_mensual_query = """
WITH CombinedTable AS (
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR)) AS mes, 'ED' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKIN
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'
    UNION ALL
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR)) AS mes, 'ED' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKOUT
    WHERE  id IS NOT NULL AND TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'
    UNION ALL
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes, 'PV' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION 
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND (paymentType = 3 OR paymentType = 4)
    UNION ALL
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes, 'CA' AS tipo_servicio
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)

SELECT mes, tipo_servicio, COUNT(*) AS count_servicio
FROM CombinedTable
WHERE mes <= EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
GROUP BY mes, tipo_servicio
ORDER BY mes, tipo_servicio;
"""

# Consulta para obtener datos de transacciones generales (semanal)
transacciones_gen_semanal_query = """
WITH CombinedTable AS (
    SELECT EXTRACT(WEEK FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS semana,
           CASE WHEN T.total != 0 AND T.total IS NOT NULL AND T.qrCode LIKE 'E%' THEN 'ED'
           END AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT EXTRACT(WEEK FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS semana,
           CASE WHEN PVT.paymentType = 3 OR PVT.paymentType = 4 THEN 'PV'
           END AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)

SELECT semana, tipo_servicio, COUNT(*) AS count_servicio
FROM CombinedTable
GROUP BY semana, tipo_servicio
ORDER BY semana, tipo_servicio;
"""

# Consulta para obtener datos de operaciones generales (semanal)
operaciones_gen_semanal_query = """
WITH CombinedTable AS (
    SELECT EXTRACT(WEEK FROM TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR)) AS semana,
           EXTRACT(MONTH FROM TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR)) AS mes,
           'ED' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKIN
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'
    UNION ALL
    SELECT EXTRACT(WEEK FROM TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR)) AS semana,
           EXTRACT(MONTH FROM TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR)) AS mes,
           'ED' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKOUT
    WHERE  id IS NOT NULL AND TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'
    UNION ALL
    SELECT EXTRACT(WEEK FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS semana,
           EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes,
           'PV' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION 
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND (paymentType = 3 OR paymentType = 4)
    UNION ALL
    SELECT EXTRACT(WEEK FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS semana,
           EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes,
           'CA' AS tipo_servicio
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
)

SELECT semana, tipo_servicio, COUNT(*) AS count_servicio
FROM CombinedTable
WHERE mes <= EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
GROUP BY semana, tipo_servicio
ORDER BY semana, tipo_servicio;
"""

# Obtener datos de transacciones y operaciones generales (mensual y semanal)
df_transacciones_general_mensual = fetch_data(transacciones_gen_mensual_query)
df_operaciones_general_mensual = fetch_data(operaciones_gen_mensual_query)
df_transacciones_general_semanal = fetch_data(transacciones_gen_semanal_query)
df_operaciones_general_semanal = fetch_data(operaciones_gen_semanal_query)

colores_oficiales = ['#EEA31B', '#030140', '#4F70B7']

# Crear gráfico de barras apiladas (mensual)
def create_stacked_bar_chart(df, tipo_servicio, periodo):
    fig = px.bar(df, x=periodo, y='count_servicio', color='tipo_servicio',
                 title=f'{tipo_servicio} - {periodo.capitalize()}',
                 labels={periodo: periodo.capitalize(), 'count_servicio': f'Cantidad de {tipo_servicio}'},
                 barmode='stack',
                 color_discrete_sequence=colores_oficiales
                 )
    return fig

# Crear gráfico interactivo para transacciones generales
st.subheader('Transacciones Generales')

# Selector para tipo de visualización (mensual o semanal)
tipo_transacciones = st.radio('Selecciona el tipo de visualización para Transacciones:',
                                  options=['Mensual', 'Semanal'])

if tipo_transacciones == 'Mensual':
    fig_transacciones = create_stacked_bar_chart(df_transacciones_general_mensual, 'Transacciones', 'mes')
else:
    fig_transacciones = create_stacked_bar_chart(df_transacciones_general_semanal, 'Transacciones', 'semana')

st.plotly_chart(fig_transacciones, use_container_width=True)

# Crear gráfico interactivo para operaciones generales
st.subheader('Operaciones Generales')

# Selector para tipo de visualización (mensual o semanal)
tipo_operaciones = st.radio('Selecciona el tipo de visualización para Operaciones:',
                                options=['Mensual', 'Semanal'])

if tipo_operaciones == 'Mensual':
    fig_operaciones = create_stacked_bar_chart(df_operaciones_general_mensual, 'Operaciones', 'mes')
else:
    fig_operaciones = create_stacked_bar_chart(df_operaciones_general_semanal, 'Operaciones', 'semana')

st.plotly_chart(fig_operaciones, use_container_width=True)


