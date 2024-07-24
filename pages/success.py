import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import plotly.express as px
from google.oauth2 import service_account

# Configuración de página
st.set_page_config(
    page_title="Kigo - Success",
    layout="wide"
)

col1, col2, col3, col4 = st.columns(4, gap="medium")

with col1:
    st.write()

with col2:
    st.image('https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/logos.png')

with col3:
    st.title('Kigo Stickiness')

with col4:
    st.write()

st.sidebar.success("PLAYBOOK.")

# API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=3600)
def get_gmv_data():
    client = bigquery.Client(credentials=credentials)

    query = """
    WITH CombinetTable AS (
        SELECT DISTINCT(transactionId) AS transactionId, total AS monto,
               EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL -6 HOUR)) AS fecha,
               'ED' AS servicio,
               CASE 
                   WHEN paymentType = 1 THEN 'NAP'
                   WHEN paymentType = 2 THEN 'SMS'
                   WHEN paymentType = 3 THEN 'TC/TD'
                   WHEN paymentType = 4 THEN 'SALDO'
                   WHEN paymentType = 5 THEN 'ATM'
                   ELSE 'Otros'
               END AS paymentType
        FROM `parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS` T
        WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND T.qrCode LIKE 'E%'
        UNION ALL
        SELECT DISTINCT(transactionId) AS transactionId, totalAmount AS monto,
               EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL -6 HOUR)) AS fecha,
               'PV' AS servicio,
               CASE 
                   WHEN paymentType = 1 THEN 'NAP'
                   WHEN paymentType = 2 THEN 'SMS'
                   WHEN paymentType = 3 THEN 'TC/TD'
                   WHEN paymentType = 4 THEN 'SALDO'
                   WHEN paymentType = 5 THEN 'ATM'
                   ELSE 'Otros'
               END AS paymentType
        FROM `parkimovil-app.cargomovil_pd.PKM_TRANSACTION` PV
        WHERE TIMESTAMP_ADD(date, INTERVAL -6 HOUR) >= '2024-01-01 00:00:00'
    ),
    PensionesData AS (
        SELECT
            EXTRACT(YEAR FROM TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR)) AS Pension_Year,
            EXTRACT(MONTH FROM TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR)) AS Pension_Month,
            SUM(IFNULL(TRN.amount, 0)) AS PENSIONES_CARD
        FROM
            `parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_LODGING_PAYMENTS` PLLP
        JOIN `parkimovil-app.cargomovil_pd.CDX_TRANSACTION` TRN
            ON PLLP.transaction_id = TRN.id
        WHERE payment_method = 'card' 
        AND TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR) >= '2024-01-01 00:00:00'
        GROUP BY Pension_Year, Pension_Month
    )

    SELECT
        EXTRACT(YEAR FROM CT.fecha) AS Year,
        EXTRACT(MONTH FROM CT.fecha) AS Month,
        CT.servicio AS Service,
        CT.paymentType AS PaymentType,
        SUM(CT.monto) AS GMV
    FROM
        CombinetTable CT
    GROUP BY
        Year, Month, Service, PaymentType
    UNION ALL
    SELECT
        PD.Pension_Year AS Year,
        PD.Pension_Month AS Month,
        'Pensiones' AS Service,
        'TC/TD' AS PaymentType,
        SUM(PD.PENSIONES_CARD) AS GMV
    FROM
        PensionesData PD
    GROUP BY
        Year, Month, Service, PaymentType
    ORDER BY
        Year, Month
    """

    query_job = client.query(query)
    results = query_job.result()

    return results.to_dataframe()


@st.cache_data(ttl=3600)
def get_gmv_data_month(selected_month):

    client = bigquery.Client(credentials=credentials)

    query = f"""
    WITH CombinetTable AS (
        SELECT DISTINCT(transactionId) AS transactionId, total AS monto,
               EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL -6 HOUR)) AS fecha,
               'ED' AS servicio,
               CASE 
                   WHEN paymentType = 1 THEN 'NAP'
                   WHEN paymentType = 2 THEN 'SMS'
                   WHEN paymentType = 3 THEN 'TC/TD'
                   WHEN paymentType = 4 THEN 'SALDO'
                   WHEN paymentType = 5 THEN 'ATM'
                   ELSE 'Otros'
               END AS paymentType
        FROM `parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS` T
        WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND T.qrCode LIKE 'E%'
        UNION ALL
        SELECT DISTINCT(transactionId) AS transactionId, totalAmount AS monto,
               EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL -6 HOUR)) AS fecha,
               'PV' AS servicio,
               CASE 
                   WHEN paymentType = 1 THEN 'NAP'
                   WHEN paymentType = 2 THEN 'SMS'
                   WHEN paymentType = 3 THEN 'TC/TD'
                   WHEN paymentType = 4 THEN 'SALDO'
                   WHEN paymentType = 5 THEN 'ATM'
                   ELSE 'Otros'
               END AS paymentType
        FROM `parkimovil-app.cargomovil_pd.PKM_TRANSACTION` PV
        WHERE TIMESTAMP_ADD(date, INTERVAL -6 HOUR) >= '2024-01-01 00:00:00'
    ),
    PensionesData AS (
        SELECT
            EXTRACT(YEAR FROM TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR)) AS Pension_Year,
            EXTRACT(MONTH FROM TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR)) AS Pension_Month,
            SUM(IFNULL(TRN.amount, 0)) AS PENSIONES_CARD
        FROM
            `parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_LODGING_PAYMENTS` PLLP
        JOIN `parkimovil-app.cargomovil_pd.CDX_TRANSACTION` TRN
            ON PLLP.transaction_id = TRN.id
        WHERE payment_method = 'card' 
        AND TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR) >= '2024-01-01 00:00:00'
        GROUP BY Pension_Year, Pension_Month
    )

    SELECT
        EXTRACT(YEAR FROM CT.fecha) AS Year,
        EXTRACT(MONTH FROM CT.fecha) AS Month,
        CT.servicio AS Service,
        CT.paymentType AS PaymentType,
        SUM(CT.monto) AS GMV
    FROM
        CombinetTable CT
    WHERE
        EXTRACT(MONTH FROM CT.fecha) = {selected_month}
    GROUP BY
        Year, Month, Service, PaymentType
    UNION ALL
    SELECT
        PD.Pension_Year AS Year,
        PD.Pension_Month AS Month,
        'Pensiones' AS Service,
        'TC/TD' AS PaymentType,
        SUM(PD.PENSIONES_CARD) AS GMV
    FROM
        PensionesData PD
    WHERE
        PD.Pension_Month = {selected_month}
    GROUP BY
        Year, Month, Service, PaymentType
    ORDER BY
        Year, Month
    """

    query_job = client.query(query).to_dataframe()

    return query_job


@st.cache_data(ttl=3600)
def gmv_hist():
    query = """
    WITH CombinetTable AS (
    SELECT
        EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL -6 HOUR)) AS fecha,
        total AS monto
    FROM `parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS` T
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    AND T.qrCode LIKE 'E%'
    UNION ALL
    SELECT
        EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL -6 HOUR)) AS Year,
        totalAmount AS monto
    FROM `parkimovil-app.cargomovil_pd.PKM_TRANSACTION` PV
    WHERE TIMESTAMP_ADD(date, INTERVAL -6 HOUR) >= '2022-01-01 00:00:00'
    ),
    PensionesData AS (
    SELECT
        EXTRACT(DATE FROM TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR)) AS Pension_Year,
        SUM(IFNULL(TRN.amount, 0)) AS PENSIONES_CARD
    FROM
        `parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_LODGING_PAYMENTS` PLLP
    JOIN `parkimovil-app.cargomovil_pd.CDX_TRANSACTION` TRN
        ON PLLP.transaction_id = TRN.id
    WHERE payment_method = 'card'
    AND TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR) >= '2022-01-01 00:00:00'
    GROUP BY Pension_Year
    )

    SELECT
    fecha,
    SUM(monto) + COALESCE(PD.PENSIONES_CARD, 0) AS GMV
    FROM
    CombinetTable CT
    LEFT JOIN
    PensionesData PD ON CT.fecha = PD.Pension_Year 
    GROUP BY
    fecha, PD.PENSIONES_CARD  -- Agregar PD.PENSIONES_CARD al GROUP BY
    ORDER BY
    fecha
    """
    df_gmv = client.query(query).to_dataframe()
    return df_gmv

df_gmv_bar = gmv_hist()


# Convertir la columna 'date' a datetime
df_gmv_bar['fecha'] = pd.to_datetime(df_gmv_bar['fecha'])

# Crear una columna con el nombre del mes
df_gmv_bar['month'] = df_gmv_bar['fecha'].dt.strftime('%B')
df_gmv_bar['year'] = df_gmv_bar['fecha'].dt.strftime('%Y')

# Agrupar por el nombre del mes y sumar los valores
df_grouped = df_gmv_bar.groupby(['month', 'year'])['GMV'].sum().reset_index()

# Renombrar las columnas
df_grouped.columns = ['month', 'year', 'gmv']

# Definir el orden de los meses
month_order = ['January', 'February', 'March', 'April', 'May', 'June',
               'July', 'August', 'September', 'October', 'November', 'December']

# Convertir la columna 'month' a un tipo categórico con el orden especificado
df_grouped['month'] = pd.Categorical(df_grouped['month'], categories=month_order, ordered=True)

# Ordenar el DataFrame por el orden de los meses
df_grouped = df_grouped.sort_values('month').reset_index(drop=True)
colores_oficiales = ['#EEA31B', '#030140', '#4F70B7']
fig_line = px.line(df_grouped, x='month', y='gmv', color='year', title='GMV Histórico',
                   labels={'month': 'Mes', 'gmv': 'GMV', 'year': 'Año'},
                   color_discrete_sequence=colores_oficiales)

st.plotly_chart(fig_line, use_container_width=True)


# Función para crear el gráfico donut sunburst con colores Kigo
def create_donut_sunburst_chart(df):
    # Paleta de colores personalizada
    colors = ["#EEA31B", "#4F70B7", "#030140"]
    color_mapping = {label: color for label, color in zip(df['Service'].unique(), colors)}

    fig = px.sunburst(df, path=['Service', 'PaymentType'], values='GMV',
                      title='Distribución de GMV por Servicio y Tipo de Pago',
                      color='Service',
                      color_discrete_map=color_mapping)
    # Ajustar el radio interior para crear el efecto de donut
    fig.update_traces(insidetextorientation='radial', textinfo='label+percent entry',
                      hoverinfo='all', branchvalues='total')

    return fig


col5, col6 = st.columns(2)

with col5:
    # Obtener datos de GMV
    df_gmv = get_gmv_data()
    # Crear gráfico donut sunburst
    fig = create_donut_sunburst_chart(df_gmv)
    # Mostrar gráfico en Streamlit
    st.plotly_chart(fig)

with col6:
    # Crear un selectbox en Streamlit para seleccionar el mes
    selected_month = st.selectbox('Selecciona un mes:', range(1, 13), index=0)
    df_gmv_month = get_gmv_data_month(selected_month)
    fig_month = create_donut_sunburst_chart(df_gmv_month)
    st.plotly_chart(fig_month)

