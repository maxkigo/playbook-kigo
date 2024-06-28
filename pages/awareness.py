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

col6, col7, col8, col9 = st.columns(4)

with col6:
    st.write()

with col7:
    st.image('https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/logos.png')

with col8:
    st.title('Kigo Awareness')

with col9:
    st.write()

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Consultas SQL
queries = {
    "MAU": {
        "general": """
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
            SELECT mes, COUNT(DISTINCT user_id) AS AU 
            FROM CombinetTable
            GROUP BY mes
            ORDER BY mes;
        """,
        "ED": """
            SELECT COUNT(DISTINCT P.phoneNumber) AS AU, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS mes
            FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
            JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
                ON T.userId = P.userId
            WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
            GROUP BY EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR))
            ORDER BY mes
        """,
        "PV": """
            SELECT COUNT(DISTINCT PF.phoneNumber) AS AU, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS mes
            FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
            JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
                ON PVT.userId = PF.userId
            WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
            GROUP BY EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR))
            ORDER BY mes
        """,
        "CA": """
            SELECT COUNT(DISTINCT user) AS AU, EXTRACT(MONTH FROM date) AS mes
            FROM parkimovil-app.geosek_raspis.log_sek
            WHERE TIMESTAMP(date) >= '2024-01-01 00:00:00'
            GROUP BY EXTRACT(MONTH FROM date)
            ORDER BY mes
        """
    },
    "WAU": {
        "general": """
            WITH CombinetTable AS (
            SELECT P.phoneNumber AS user_id, EXTRACT(WEEK FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS semana
            FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
            JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
                ON T.userId = P.userId
            WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
            UNION ALL
            SELECT PF.phoneNumber AS user_id, EXTRACT(WEEK FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS semana
            FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
            JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
                ON PVT.userId = PF.userId
            WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
            UNION ALL 
            SELECT user AS user_id, EXTRACT(WEEK FROM date) AS semana
            FROM parkimovil-app.geosek_raspis.log_sek
            WHERE TIMESTAMP(date) >= '2024-01-01 00:00:00'
            )
            SELECT semana, COUNT(DISTINCT user_id) AS AU 
            FROM CombinetTable
            GROUP BY semana
            ORDER BY semana;
        """,
        "ED": """
            SELECT COUNT(DISTINCT P.phoneNumber) AS AU, EXTRACT(WEEK FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS semana
            FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
            JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
                ON T.userId = P.userId
            WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
            GROUP BY EXTRACT(WEEK FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR))
            ORDER BY semana
        """,
        "PV": """
            SELECT COUNT(DISTINCT PF.phoneNumber) AS AU, EXTRACT(WEEK FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS semana
            FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
            JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
                ON PVT.userId = PF.userId
            WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
            GROUP BY EXTRACT(WEEK FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR))
            ORDER BY semana
        """,
        "CA": """
            SELECT COUNT(DISTINCT user) AS AU, EXTRACT(WEEK FROM date) AS semana
            FROM parkimovil-app.geosek_raspis.log_sek
            WHERE TIMESTAMP(date) >= '2024-01-01 00:00:00'
            GROUP BY EXTRACT(WEEK FROM date)
            ORDER BY semana
        """
    }
}

# Consultas SQL para DAU
@st.cache_data(ttl=3600)
def dau():
        dau_queries = """
        WITH CombinetTable AS (
        SELECT P.phoneNumber AS user_id, EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS dia, 'ED' AS servicio
        FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
        JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
            ON T.userId = P.userId
        WHERE EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        UNION ALL
        SELECT PF.phoneNumber AS user_id, EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS dia, 'PV' AS servicio
        FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
        JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
            ON PVT.userId = PF.userId
        WHERE EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        UNION ALL 
        SELECT user AS user_id, DATE(date) AS dia, 'CA' AS servicio
        FROM parkimovil-app.geosek_raspis.log_sek
        WHERE EXTRACT(DATE FROM TIMESTAMP(date)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        )
        SELECT dia, servicio, COUNT(DISTINCT user_id) AS DAU
        FROM CombinetTable
        GROUP BY dia, servicio
        ORDER BY dia;
        """
        df_dau = client.query(dau_queries).to_dataframe()
        return df_dau

df_dau = dau()

# Selección de MAU/WAU
metrica_seleccionada = st.selectbox('Selecciona una métrica:', ['MAU', 'WAU'])

# Selección de servicio
servicios = ['Todos los Servicios', 'PV', 'ED', 'CA']
servicio_seleccionado = st.selectbox('Selecciona un servicio:', servicios)

# Obtener la consulta correspondiente
if servicio_seleccionado == 'Todos los Servicios':
    query = queries[metrica_seleccionada]["general"]
else:
    query = queries[metrica_seleccionada][servicio_seleccionado]

# Ejecutar la consulta y obtener el DataFrame
df_mau_general = client.query(query).to_dataframe()

# Calcular la predicción usando ARIMA
model = ARIMA(df_mau_general['AU'].iloc[0:-1], order=(3, 2, 3))
model_fit = model.fit()
forecast = model_fit.forecast(steps=1)
prediccion = forecast.iloc[0].astype('int64')

# Crear la figura con Plotly
fig = go.Figure()

# Añadir barras al gráfico
if metrica_seleccionada == 'MAU':
    fig.add_trace(go.Bar(
        x=df_mau_general['mes'],
        y=df_mau_general['AU'],
        name='MAU',
        marker_color='#0F0068'
    ))
    # Logo Kigo
    fig.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
    ultimo_valor = df_mau_general['mes'].iloc[-1]
    titulo = 'Proyección de MAU para el Mes en Curso'
    xaxis_title = 'Mes'

    # Mostrar el MAU actual y la proyección como indicadores
    mau_actual = df_mau_general['AU'].iloc[-1]
    mau_lastweek = df_mau_general['AU'].iloc[-2]
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label='MAU Mes Pasado', value=mau_lastweek)
    with col2:
        st.metric(label="MAU Actual", value=mau_actual)
    with col3:
        st.metric(label="Proyección de MAU", value=prediccion)
else:
    fig.add_trace(go.Bar(
        x=df_mau_general['semana'],
        y=df_mau_general['AU'],
        name='WAU',
        marker_color='#0F0068'
    ))
    # Logo Kigo
    fig.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )
    ultimo_valor = df_mau_general['semana'].iloc[-1]
    titulo = 'Proyección de WAU para la Semana en Curso'
    xaxis_title = 'Semana'

    # Mostrar el WAU actual y la proyección como indicadores
    wau_actual = df_mau_general['AU'].iloc[-1]
    wau_lastweek = df_mau_general['AU'].iloc[-2]
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="WAU Semana Pasada", value=wau_lastweek)
    with col2:
        st.metric(label="WAU Actual", value=wau_actual)
    with col3:
        st.metric(label="Proyección de WAU", value=prediccion)

# Resaltar el último valor con un color diferente
fig.add_trace(go.Bar(
    x=[ultimo_valor],
    y=[prediccion],
    marker=dict(color='red'),  # Cambiar el color del último valor
    name='Proyección',
    marker_color='#F24405'
))

# Actualizar diseño del gráfico
fig.update_layout(
    title=titulo,
    xaxis_title=xaxis_title,
    yaxis_title=metrica_seleccionada
)

st.plotly_chart(fig, use_container_width=True)



# Paleta de colores oficial
colores_oficiales = ['#EEA31B', '#030140', '#4F70B7']

# DAU Gráfico de Área con Selección de Servicios y colores oficiales
fig_dau_area = px.area(df_dau, x='dia', y='DAU', color='servicio', title='DAU por Servicio Acomulado',
                       labels={'dia': 'Día', 'DAU': 'DAU', 'servicio': 'Servicio'},
                       category_orders={"servicio": ["ED", "PV", "CA"]},
                       color_discrete_sequence=colores_oficiales)
fig_dau_area.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )

# Calcular el total de DAU para cada día
df_dau_total = df_dau.groupby('dia')['DAU'].sum().reset_index()

# Unirse a df_dau_total para obtener el total de DAU para cada día
df_dau_merged = df_dau.merge(df_dau_total, on='dia', suffixes=('', '_total'))

# Calcular el porcentaje de DAU para cada servicio en relación con el total
df_dau_merged['porcentaje'] = (df_dau_merged['DAU'] / df_dau_merged['DAU_total']) * 100

# Gráfico de Barras Empalmadas para DAU Dividido por Servicio con Porcentaje
fig_dau_barras = px.bar(df_dau_merged, x='dia', y='DAU', color='servicio', title='DAU por Servicio Porcentaje',
                        labels={'dia': 'Día', 'DAU': 'DAU', 'servicio': 'Servicio'},
                        category_orders={"servicio": ["ED", "PV", "CA"]},
                        hover_data={"porcentaje": True},
                        text='porcentaje',
                        color_discrete_sequence=colores_oficiales
                        )
fig_dau_barras.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
fig_dau_barras.add_layout_image(
        dict(
            source="https://www.kigo.pro/recursos-kigo/img-kigo/kigo-logo.png",
            xref="paper", yref="paper",
            x=1, y=1.05,
            sizex=0.2, sizey=0.2,
            xanchor="right", yanchor="bottom"
        )
    )

col10, col11 = st.columns(2)
with col10:
    st.plotly_chart(fig_dau_area, use_container_width=True)
with col11:
    st.plotly_chart(fig_dau_barras, use_container_width=True)

# Consultas SQL para DAU de todos los servicios
@st.cache_data(ttl=3600)
def dau_query():
    query = """
    WITH CombinetTable AS (
    SELECT P.phoneNumber AS user_id, EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS dia
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
        ON T.userId = P.userId
    WHERE EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
    UNION ALL
    SELECT PF.phoneNumber AS user_id, EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS dia
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
        ON PVT.userId = PF.userId
    WHERE EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
    UNION ALL 
    SELECT user AS user_id, DATE(date) AS dia
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE EXTRACT(DATE FROM TIMESTAMP(date)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
    )
    SELECT dia, COUNT(DISTINCT user_id) AS DAU 
    FROM CombinetTable
    GROUP BY dia
    ORDER BY dia;
    """
    df_dau = client.query(query).to_dataframe()
    return df_dau


df_dau = dau_query()
# Calcular el primer y último día del mes pasado
fecha_actual = datetime.now()
primer_dia_mes_actual = fecha_actual.replace(day=1)
primer_dia_mes_pasado = (primer_dia_mes_actual - timedelta(days=1)).replace(day=1)
ultimo_dia_mes_pasado = primer_dia_mes_actual - timedelta(days=1)

# Filtrar el DataFrame para obtener solo los datos del mes pasado
df_mes_pasado = df_dau[(df_dau['dia'] >= primer_dia_mes_pasado) & (df_dau['dia'] <= ultimo_dia_mes_pasado)]

# Calcular el DAU promedio del mes pasado
if not df_mes_pasado.empty:
    dau_promedio_mes_pasado = round(df_mes_pasado['DAU'].mean(), 2)
else:
    dau_promedio_mes_pasado = "No hay suficientes datos"

# Calcular el primer día del mes en curso
dia_anterior = fecha_actual - timedelta(days=1)

# Filtrar el DataFrame para obtener solo los datos del mes en curso hasta el día anterior al día de la consulta
df_mes_actual = df_dau[(df_dau['dia'] >= primer_dia_mes_actual) & (df_dau['dia'] <= dia_anterior)]

# Calcular el DAU promedio del mes en curso hasta el día anterior al día de la consulta
if not df_mes_actual.empty:
    dau_promedio_mes_actual = round(df_mes_actual['DAU'].mean(), 2)
else:
    dau_promedio_mes_actual = "No hay suficientes datos"


col13, col14, col15 = st.columns(3)
with col13:
    # Mostrar los indicadores
    st.metric(label="DAU Promedio Mes Pasado", value=dau_promedio_mes_pasado)
with col14:
    st.metric(label="DAU Promedio Mes en Curso", value=dau_promedio_mes_actual)
with col15:
    st.metric(label='DAU/MAU Radio', value=round((dau_promedio_mes_pasado / 319278) * 100, 2))