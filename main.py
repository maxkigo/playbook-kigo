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
    page_title="Kigo - Playbook Kigo",
    page_icon="👋",
    layout="wide"
)

st.sidebar.success("PLAYBOOK.")

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


@st.cache_data(ttl=3600)
def mau_general():
    mau_general = """
    WITH CombinetTable AS (
    SELECT P.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
        ON T.userId = P.userId
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL
    SELECT PF.phoneNumber AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE PF
        ON PVT.userId = PF.userId
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    UNION ALL 
    SELECT user AS user_id, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    )
    
    SELECT month, COUNT(DISTINCT user_id) AS MAU 
    FROM CombinetTable
    GROUP BY month
    ORDER BY month;
    """

    df_mau_general = client.query(mau_general).to_dataframe()
    return df_mau_general


df_mau_general = mau_general()
mau_last_month = df_mau_general['MAU'].iloc[-3]
mau_actual = df_mau_general['MAU'].iloc[-2]


@st.cache_data(ttl=3600)
def gmv_general():
    gmv_general = """
    WITH CombinetTable AS (
    SELECT DISTINCT(transactionId) AS transactionId, total AS monto,
           EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL -6 HOUR)) AS fecha
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND T.qrCode LIKE 'E%'
    UNION ALL
    SELECT DISTINCT(transactionId) AS transactionId, totalAmount AS monto,
           EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL -6 HOUR)) AS fecha
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PV
    WHERE TIMESTAMP_ADD(date, INTERVAL -6 HOUR) >= '2024-01-01 00:00:00'
    ),
    PensionesData AS (
    SELECT
        EXTRACT(YEAR FROM TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR)) AS Pension_Year,
        EXTRACT(MONTH FROM TIMESTAMP_ADD(CAST(PLLP.charge_date AS TIMESTAMP), INTERVAL -6 HOUR)) AS Pension_Month,
        SUM(IFNULL(TRN.amount, 0)) AS PENSIONES_CARD
    FROM
        parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_LODGING_PAYMENTS PLLP
    JOIN parkimovil-app.cargomovil_pd.CDX_TRANSACTION TRN
        ON PLLP.transaction_id = TRN.id
    WHERE payment_method = 'card'
    GROUP BY Pension_Year, Pension_Month
    )

    SELECT
    EXTRACT(YEAR FROM CT.fecha) AS Year,
    EXTRACT(MONTH FROM CT.fecha) AS mes,
    SUM(CT.monto) + IFNULL(PD.PENSIONES_CARD, 0) AS GMV
    FROM
    CombinetTable CT
    LEFT JOIN
    PensionesData PD ON EXTRACT(YEAR FROM CT.fecha) = PD.Pension_Year
                    AND EXTRACT(MONTH FROM CT.fecha) = PD.Pension_Month
    GROUP BY
    Year, mes, PD.PENSIONES_CARD
    ORDER BY
    Year, mes
    """
    df_gmv_general = client.query(gmv_general).to_dataframe()
    return df_gmv_general

df_gmv_general = gmv_general()
gmv_last_month = df_gmv_general['GMV'].iloc[-3]
gmv_actual = df_gmv_general['GMV'].iloc[-2]

# TRANSACCIONES GENERALES
@st.cache_data(ttl=3600)
def transacciones_gen():
    transacciones_gen = """
    -- APP
    WITH CombinetTable AS (
    SELECT T.transactionId AS transacciones, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND T.total != 0 
    AND T.total IS NOT NULL AND T.qrCode LIKE 'E%'
    UNION ALL
    SELECT PVT.id AS transacciones, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION PVT
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' 
    AND (PVT.paymentType = 3 OR PVT.paymentType = 4) 
    AND PVT.amount != 0 AND PVT.amount IS NOT NULL
    )
    
    SELECT month, COUNT(DISTINCT transacciones) AS Transacciones
    FROM CombinetTable
    GROUP BY month
    ORDER BY month
    """
    df_transacciones_general = client.query(transacciones_gen).to_dataframe()
    return df_transacciones_general


df_transacciones_general = transacciones_gen()
trans_last_month = df_transacciones_general['Transacciones'].iloc[-3]
trans_actural = df_transacciones_general['Transacciones'].iloc[-2]

# OPERACIONES GENERALES
@st.cache_data(ttl=3600)
def operaciones_gen():
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
    df_operaciones_general = client.query(operaciones_gen).to_dataframe()
    return df_operaciones_general


df_operaciones_general = operaciones_gen()
op_last_month = df_operaciones_general['Operaciones'].iloc[-5]
op_actual = df_operaciones_general['Operaciones'].iloc[-4]

@st.cache_data(ttl=3600)
def usuarios_multiservicio():
    usuarios_multiservicio = """
    WITH usuariosTableED AS (
    SELECT S.phoneNumber AS user_id, T.transactionId AS Operacion, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    ),
    usuariosTablePV AS (
    SELECT S.phoneNumber AS user_id, T.transactionId AS Operacion, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S
        ON T.userId = S.userId
    WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    ),
    usuariosTableCA AS (
    SELECT user AS user_id, idlog AS operacion, EXTRACT(MONTH FROM TIMESTAMP_ADD(date, INTERVAL - 6 HOUR)) AS month
    FROM parkimovil-app.geosek_raspis.log_sek
    WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    )

    SELECT month, COUNT(*) as users_multi
    FROM (
    SELECT user_id, month, COUNT(*) as appearances
    FROM (
        SELECT distinct user_id, month FROM usuariosTableED
        UNION ALL
        SELECT distinct user_id, month FROM usuariosTablePV
        UNION ALL
        SELECT distinct user_id, month FROM usuariosTableCA
    ) all_users
    GROUP BY user_id, month
    ) multiple_appearances
    WHERE appearances > 1
    GROUP BY month
    ORDER BY month;       
    """
    df_multiservicio_gen = client.query(usuarios_multiservicio).to_dataframe()
    return df_multiservicio_gen


df_multiservicio_gen = usuarios_multiservicio()
multiservicio_gen_last_month = df_multiservicio_gen['users_multi'].iloc[-3]
multivervicio_actual = df_multiservicio_gen['users_multi'].iloc[-2]

porcen_multi = (multivervicio_actual * 100) / mau_actual
porcen_multi_last = (multiservicio_gen_last_month * 100) / mau_last_month


@st.cache_data(ttl=3600)
def usuarios_multiproyecto():
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
    return df_multiproyecto_gen


df_multiproyecto_gen = usuarios_multiproyecto()
multiproyecto_gen_last_month = df_multiproyecto_gen['usuarios_multiservicio_por_mes'].iloc[-3]
multiproyecto_actual = df_multiproyecto_gen['usuarios_multiservicio_por_mes'].iloc[-2]

porcen_proyec = (multiproyecto_actual * 100) / mau_actual
porcen_proyec_last = (multiproyecto_gen_last_month * 100) / mau_last_month

# POU GENERAL
@st.cache_data(ttl=3600)
def pou_general():
    pou_general = """
    SELECT COUNT(DISTINCT qr) AS POU
    FROM parkimovil-app.geosek_raspis.raspis
    WHERE bridge IN (2, 3, 4, 6, 7, 8, 9, 10, 12, 13, 15, 16, 17, 18, 20, 21, 22, 25, 26, 29) AND monitor_active = 1;
    """
    pou_gen = client.query(pou_general).to_dataframe()
    return pou_gen


pou_gen = pou_general()
pou_gen = pou_gen['POU'].iloc[-1]

# PAISES CON PRESENCIA KIGO
num_paises = 3

#CIUDADES CON PRESENCIA KIGO
num_ciudades = 56


# Proyectos Activos
@st.cache_data(ttl=3600)
def proyectos():
    proyectos = """
    WITH proyectos_activiti AS (
    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(TED.paymentDate, INTERVAL -6 HOUR)) AS last_mothn, (COUNT(TED.transactionId) * 2) AS operaciones, CATED.parkingLotName AS proyecto
    FROM `parkimovil-app`.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS TED
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_LOT_CAT CATED
    on TED.parkingLotId = CATED.id
    WHERE EXTRACT(MONTH FROM TIMESTAMP_ADD(TED.paymentDate, INTERVAL -6 HOUR)) = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AND EXTRACT(DATE FROM TIMESTAMP_ADD(paymentDate, INTERVAL -6 HOUR)) >= '2024-01-01'
    GROUP BY EXTRACT(MONTH FROM TIMESTAMP_ADD(TED.paymentDate, INTERVAL -6 HOUR)), CATED.parkingLotName

    UNION ALL

    SELECT EXTRACT(MONTH FROM TIMESTAMP_ADD(TPV.date, INTERVAL -6 HOUR)) AS last_mothn, COUNT(TPV.transactionId) AS operaciones, PVCAT.name AS proyecto
    FROM `parkimovil-app`.cargomovil_pd.PKM_TRANSACTION TPV
    JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT PVCAT
    ON TPV.zoneId = PVCAT.id
    WHERE EXTRACT(MONTH FROM TIMESTAMP_ADD(TPV.date, INTERVAL -6 HOUR)) = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AND EXTRACT(DATE FROM TIMESTAMP_ADD(date, INTERVAL -6 HOUR)) >= '2024-01-01'
    GROUP BY EXTRACT(MONTH FROM TIMESTAMP_ADD(TPV.date, INTERVAL -6 HOUR)), PVCAT.name

    UNION ALL

    SELECT EXTRACT(MONTH FROM S.date) AS last_mothn, COUNT(S.function_) AS operaciones, R.alias AS proyecto
    FROM `parkimovil-app`.geosek_raspis.log_sek S
    JOIN `parkimovil-app`.geosek_raspis.raspis R
    ON S.QR = R.qr
    WHERE EXTRACT(MONTH FROM S.date) = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) AND EXTRACT(DATE FROM date) >= '2024-01-01'
    GROUP BY EXTRACT(MONTH FROM S.date), R.alias
    )

    SELECT COUNT(proyecto)
    FROM proyectos_activiti
    WHERE operaciones > 50
    """
    proyectos_activos_mes = client.query(proyectos).to_dataframe()
    return proyectos_activos_mes


proyectos_activos_mes = proyectos()
proyectos_activos_mes = proyectos_activos_mes.iat[0, 0]


# DESCARGAS IOS Y ANDROID
descargas_abril = 72396
descargas_marzo = 32350 + 39439
descargas_junio = 35738 + 40632

# ---------------------------------------------------------INDICADORES Y GRÁFICAS------------------------------------------------------------------------------------

# INDICADOR MAU GENERAL
# Delta del Indicador
fig_mau_gen = go.Figure(go.Indicator(
    mode="number",
    value=mau_actual,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "MAU", 'font': {'color': "#F24405"}, 'align': 'center'},
    delta={'position': "bottom", 'reference': mau_last_month}
))

# Actualizar el diseño del indicador
fig_mau_gen.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),
)

# Ajustar bordes redondeados y color del borde
fig_mau_gen.update_traces(title_font=dict(size=14))
fig_mau_gen.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# INDICADOR GMV GENERAL
fig_gmv_gen = go.Figure(go.Indicator(
    mode="number",
    value=gmv_actual + 4861524,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "GMV", 'font': {'color': "#F24405"}, 'align': 'center'}
))

# Actualizar el diseño del indicador
fig_gmv_gen.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_gmv_gen.update_traces(title_font=dict(size=14))
fig_gmv_gen.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# INDICADOR TRANSACCIONES GENERAL
fig_tran_gen = go.Figure(go.Indicator(
    mode="number",
    value=trans_actural,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Transacciones", 'font': {'color': "#F24405"}, 'align': 'center'},
    delta={'position': "bottom", 'reference': trans_last_month}
))

fig_tran_gen.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_tran_gen.update_traces(title_font=dict(size=14))
fig_tran_gen.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# INDICADOR DE OPERACIONES GENERAL
fig_ope_gen = go.Figure(go.Indicator(
    mode="number",
    value=op_actual,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Operaciones", 'font': {'color': "#F24405"}, 'align': 'center'},
    delta={'position': "bottom", 'reference': op_last_month}
))

fig_ope_gen.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_ope_gen.update_traces(title_font=dict(size=14))
fig_ope_gen.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# INDICADOR DE USUARIOS MULTI-SERVICIO
fig_multi_gen = go.Figure(go.Indicator(
    mode="number",
    value=porcen_multi,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Usuarios Multi-Servicio", 'font': {'color': "#F24405"}, 'align': 'center'},
    delta={'position': "bottom", 'reference': porcen_multi_last},
    number={'suffix': "%"}
))

fig_multi_gen.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_multi_gen.update_traces(title_font=dict(size=14))
fig_multi_gen.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

#INDICADORES MULTI-PROYECTO
fig_multipro_gen = go.Figure(go.Indicator(
    mode="number",
    value=porcen_proyec,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Usuarios Multi-Proyecto", 'font': {'color': "#F24405"}, 'align': 'center'},
    delta={'position': "bottom", 'reference': porcen_proyec_last},
    number={'suffix': "%"}
))

fig_multipro_gen.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_multipro_gen.update_traces(title_font=dict(size=14))
fig_multipro_gen.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))


# POU GENERAL
fig_pou_gen = go.Figure(go.Indicator(
    mode="number",
    value=pou_gen,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "POU", 'font': {'color': "#F24405"}, 'align': 'center'},
))

fig_pou_gen.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_pou_gen.update_traces(title_font=dict(size=14))
fig_pou_gen.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# NUMERO DE PAISES
fig_paises = go.Figure(go.Indicator(
    mode="number",
    value=num_paises,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Países", 'font': {'color': "#F24405"}, 'align': 'center'},
))

fig_paises.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_paises.update_traces(title_font=dict(size=14))
fig_paises.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# NUMERO DE CIUDADES
fig_ciudades = go.Figure(go.Indicator(
    mode="number",
    value=num_ciudades,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Ciudades", 'font': {'color': "#F24405"}, 'align': 'center'},
))

fig_ciudades.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_ciudades.update_traces(title_font=dict(size=14))
fig_ciudades.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# PROYECTOS ACTIVOS
fig_proyectos = go.Figure(go.Indicator(
    mode="number",
    value=proyectos_activos_mes,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Proyectos Activos", 'font': {'color': "#F24405"}, 'align': 'center'},
))

fig_proyectos.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_proyectos.update_traces(title_font=dict(size=14))
fig_proyectos.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# DESCARGAS
# Delta del Indicador
fig_descargas = go.Figure(go.Indicator(
    mode="number",
    value=descargas_junio,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Descargas", 'font': {'color': "#F24405"}, 'align': 'center'},
    delta = {'position': "bottom", 'reference': descargas_marzo}
))

# Actualizar el diseño del indicador
fig_descargas.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_descargas.update_traces(title_font=dict(size=14))
fig_descargas.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# Gross Revenue
# Delta del Indicador
fig_revenue = go.Figure(go.Indicator(
    mode="number",
    value= 7547358.04,
    domain={'x': [0, 1], 'y': [0, 1]},
    title={'text': "Gross Revenue", 'font': {'color': "#F24405"}, 'align': 'center'}
))

# Actualizar el diseño del indicador
fig_revenue.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    plot_bgcolor="rgba(0,0,0,0)",  # Fondo transparente
    margin=dict(l=20, r=20, t=20, b=20),  # Márgenes ajustados
    height=100,  # Altura ajustada
    width=200,  # Ancho ajustado
    font=dict(color="#F24405"),  # Color del texto
)

# Ajustar bordes redondeados y color del borde
fig_revenue.update_traces(title_font=dict(size=14))
fig_revenue.update_traces(gauge=dict(axis=dict(tickcolor="#F24405", tick0=2)))

# ------------------------------proyecciones----------------------------------------------------------------------------

# Style CSS

custom_css = """
<style>
  .custom-title {
    font-size: 18px; /* Cambia el tamaño de la fuente según tus necesidades */
    text-align: center; /* Centrar el texto */
    margin-bottom: 10px; /* Espacio inferior entre el título y el gráfico */
  }
  .custom-container {
    display: flex;
    flex-direction: column;
    align-items: center; /* Centrar el contenido verticalmente */
  }
</style>
"""

# Insert the custom CSS styles
st.markdown(custom_css, unsafe_allow_html=True)

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.markdown('<div class="custom-container"><h1 class="custom-title">Footprint</h1>', unsafe_allow_html=True)
    st.plotly_chart(fig_paises)
    st.plotly_chart(fig_ciudades)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - MH</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - AO</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - BJ</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - Cuauh</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Juárez</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">Ecatepec</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">Mexicali</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">Mérida</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">San Luis Potosí</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">Culiacán</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - Tlalpan</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - Azcapotzalco</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">Culiacán</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - Iztap</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="custom-container"><h1 class="custom-title">Projects</h1>', unsafe_allow_html=True)
    st.plotly_chart(fig_proyectos)
    st.plotly_chart(fig_pou_gen)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto; 
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Guadalajara</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - Cuajimalpa</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto; 
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - GAM</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Tijuana</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto; 
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM León</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Veracruz</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Cancún</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Chihuahua</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Tuxtla</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Querétaro</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - Naulcalpan</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">CDMX - Coyo</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Torreón</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="custom-container"><h1 class="custom-title">Awareness</h1>', unsafe_allow_html=True)
    st.plotly_chart(fig_mau_gen)
    st.plotly_chart(fig_descargas)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto; 
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Monterrey</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Hermosillo</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    st.markdown('<div class="custom-container"><h1 class="custom-title">Engagement</h1>', unsafe_allow_html=True)
    st.plotly_chart(fig_multi_gen)
    st.plotly_chart(fig_multipro_gen)

    st.markdown('</div>', unsafe_allow_html=True)

with col5:
    st.markdown('<div class="custom-container"><h1 class="custom-title">Stickiness</h1>', unsafe_allow_html=True)
    st.plotly_chart(fig_ope_gen)
    st.plotly_chart(fig_tran_gen)
    st.markdown(
        '''
        <div style="
            border: 2px solid grey;
            border-radius: 10px;
            padding: 10px;
            margin: 0 auto;
            width: fit-content;
            background-color: grey;
        ">
            <p style="
                text-align: center; 
                margin: 0; 
                color: white; 
                font-weight: bold;
                font-size: 16px;
            ">ZM Puebla</p>
        </div>
        ''',
        unsafe_allow_html=True
    )
    st.markdown('</div>', unsafe_allow_html=True)

with col6:
    st.markdown('<div class="custom-container"><h1 class="custom-title">Success</h1>', unsafe_allow_html=True)
    st.plotly_chart(fig_gmv_gen)
    st.plotly_chart(fig_revenue)
    st.markdown('</div>', unsafe_allow_html=True)

background_image_url = "https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/bg_playbook_4.jpg"
page_bg_img = f'''
<style>
  .stApp {{
    background-image: url("{background_image_url}");
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
  }}
</style>
'''

st.markdown(page_bg_img, unsafe_allow_html=True)