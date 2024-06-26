import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

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
@st.cache_data(ttl=3600)
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
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND (paymentType = 3 OR 
    paymentType = 4)
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

@st.cache_data(ttl=3600)
def get_operations_services():
    query = """
    WITH CombinedTable AS (
    SELECT EXTRACT(DATE FROM TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR)) AS fecha, 'ED' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKIN
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(checkinDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'
    UNION ALL
    SELECT EXTRACT(DATE FROM TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR)) AS fecha, 'ED' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_CHECKOUT
    WHERE  id IS NOT NULL AND TIMESTAMP_ADD(checkOutDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' 
    AND qrCode LIKE 'E%'
    UNION ALL
    SELECT EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS fecha, 'PV' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION 
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' 
    AND (paymentType = 3 OR paymentType = 4)
    UNION ALL
    SELECT EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS fecha, 'CA' AS tipo_servicio
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    )
    
    SELECT
        EXTRACT(YEAR FROM CT.fecha) AS Year,
        EXTRACT(MONTH FROM CT.fecha) AS Month,
        CT.tipo_servicio AS Service,
        COUNT(*) AS Operaciones
    FROM
        CombinedTable CT
    WHERE EXTRACT(MONTH FROM CT.fecha) <= EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
    GROUP BY
        Year, Month, CT.tipo_servicio
    """

    query_job = client.query(query)
    results = query_job.result()

    return results.to_dataframe()


@st.cache_data(ttl=3600)
def get_transaction_services():
    query = """
    WITH CombinedTable AS (
    SELECT EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS fecha, 'ED' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS 
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND qrCode LIKE 'E%'
    UNION ALL
    SELECT EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS fecha, 'PV' AS tipo_servicio
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION 
    WHERE id IS NOT NULL AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' 
    AND (paymentType = 3 OR paymentType = 4)
    )

    SELECT
        EXTRACT(YEAR FROM CT.fecha) AS Year,
        EXTRACT(MONTH FROM CT.fecha) AS Month,
        CT.tipo_servicio AS Service,
        COUNT(*) AS Transacciones
    FROM
        CombinedTable CT
    WHERE EXTRACT(MONTH FROM CT.fecha) <= EXTRACT(MONTH FROM CURRENT_TIMESTAMP)
    GROUP BY
        Year, Month, CT.tipo_servicio
    """

    query_job = client.query(query)
    results = query_job.result()

    return results.to_dataframe()

# Crear gráfico interactivo para transacciones generales
st.subheader('Transacciones Generales')

# Selector para tipo de visualización (mensual o semanal)
tipo_transacciones = st.radio('Selecciona el tipo de visualización para Transacciones:',
                                  options=['Mensual', 'Semanal'])

if tipo_transacciones == 'Mensual':
    fig_transacciones = create_stacked_bar_chart(df_transacciones_general_mensual, 'Transacciones',
                                                 'mes')
    fig_transacciones.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
else:
    fig_transacciones = create_stacked_bar_chart(df_transacciones_general_semanal, 'Transacciones',
                                                 'semana')
    fig_transacciones.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )

st.plotly_chart(fig_transacciones, use_container_width=True)

# Crear gráfico interactivo para operaciones generales
st.subheader('Operaciones Generales')

# Selector para tipo de visualización (mensual o semanal)
tipo_operaciones = st.radio('Selecciona el tipo de visualización para Operaciones:',
                                options=['Mensual', 'Semanal'])

if tipo_operaciones == 'Mensual':
    fig_operaciones = create_stacked_bar_chart(df_operaciones_general_mensual, 'Operaciones', 'mes')
    fig_operaciones.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
else:
    fig_operaciones = create_stacked_bar_chart(df_operaciones_general_semanal, 'Operaciones', 'semana')
    fig_operaciones.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )

st.plotly_chart(fig_operaciones, use_container_width=True)


# Función para crear el gráfico circular
def create_pie_chart(df):
    current_year = datetime.now().year
    oper_by_service = df.groupby('Service')['Operaciones'].sum().reset_index()
    fig = px.pie(oper_by_service, names='Service', values='Operaciones',
                 title=f'Porcentaje de Operaciones por Servicio - ({current_year})',
                 color_discrete_sequence=colores_oficiales)
    fig.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
    return fig


def create_pie_chart_tran(df):
    current_year = datetime.now().year
    oper_by_service = df.groupby('Service')['Transacciones'].sum().reset_index()
    fig = px.pie(oper_by_service, names='Service', values='Transacciones',
                 title=f'Porcentaje de Transacciones por Servicio - ({current_year})',
                 color_discrete_sequence=['#4F70B7', '#030140'])
    fig.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
    return fig


df_oper_serv = get_operations_services()
df_tran_serv = get_transaction_services()

pie_operaciones = create_pie_chart(df_oper_serv)
pie_transacciones = create_pie_chart_tran(df_tran_serv)
st.plotly_chart(pie_operaciones, use_container_width=True)
st.plotly_chart(pie_transacciones, use_container_width=True)
