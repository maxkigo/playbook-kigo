import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="Kigo - Stickiness",
    layout="wide"
)

# Layout and Header
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.write()
with col2:
    st.image('https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/logos.png')
with col3:
    st.title('Kigo Projects')
with col4:
    st.write()

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


@st.cache_data(ttl=3600)
def tree_map_proyects():
    query_ed_estado = """
    SELECT ST.name AS state, 
           CASE 
               WHEN CI.name LIKE 'Puebla%' THEN 'Puebla'
               ELSE CI.name 
           END AS ciudad, 
           CASE 
               WHEN CAT.parkingLotName LIKE '%LOMAS%' THEN 'Lomas de Angelopolis'
               ELSE CAT.parkingLotName
           END AS proyecto, 
           COUNT(DISTINCT T.userId) AS usuarios, 
           EXTRACT(MONTH FROM paymentDate) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkinglotId = CAT.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT CI
        ON CAT.cityId = CI.id
    JOIN parkimovil-app.cargomovil_pd.GEN_STATE_PROVINCE_CAT ST
        ON CI.stateId = ST.id
    WHERE EXTRACT(YEAR FROM paymentDate) = 2024
    GROUP BY ST.name, CASE WHEN CI.name LIKE 'Puebla%' THEN 'Puebla' ELSE CI.name END, CASE WHEN CAT.parkingLotName LIKE '%LOMAS%' THEN 'Lomas de Angelopolis' ELSE CAT.parkingLotName END, EXTRACT(MONTH FROM paymentDate)

    UNION ALL

    SELECT ST.name AS state, 
           CASE 
               WHEN CI.name LIKE 'Puebla%' THEN 'Puebla'
               ELSE CI.name 
           END AS ciudad, 
           CASE 
               WHEN CAT.name LIKE '%LOMAS%' THEN 'Lomas de Angelopolis'
               ELSE CONCAT(CAT.name, ' PV') 
           END AS proyecto, 
           COUNT(DISTINCT T.userId) AS usuarios, 
           EXTRACT(MONTH FROM date) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
    JOIN parkimovil-app.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT CAT
        ON T.zoneid = CAT.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT CI
        ON CAT.cityId = CI.id
    JOIN parkimovil-app.cargomovil_pd.GEN_STATE_PROVINCE_CAT ST
        ON CI.stateId = ST.id
    WHERE EXTRACT(YEAR FROM date) = 2024
    GROUP BY ST.name, CASE WHEN CI.name LIKE 'Puebla%' THEN 'Puebla' ELSE CI.name END, CASE WHEN CAT.name LIKE '%LOMAS%' THEN 'Lomas de Angelopolis' ELSE CONCAT(CAT.name, ' PV') END, EXTRACT(MONTH FROM date)

    UNION ALL 

    SELECT ZM.estado AS state, 
           CASE 
               WHEN CACY.ciudad LIKE 'Puebla%' THEN 'Puebla'
               ELSE CACY.ciudad 
           END AS ciudad, 
           CASE 
               WHEN R.alias LIKE '%LOMAS%' THEN 'Lomas de Angelopolis'
               ELSE R.alias 
           END AS proyecto, 
           COUNT(DISTINCT L.user) AS usuarios, 
           EXTRACT(MONTH FROM date) AS mes
    FROM parkimovil-app.geosek_raspis.log_sek L
    JOIN parkimovil-app.geosek_raspis.raspis R
        ON L.QR = R.qr
    JOIN parkimovil-app.cargomovil_pd.ca_citys CACY
        ON R.alias = CACY.proyecto 
    JOIN parkimovil-app.cargomovil_pd.zona_metropolitana_new ZM  
        ON CACY.ciudad = ZM.municipio
    WHERE TIMESTAMP_ADD(L.date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    GROUP BY ZM.estado, CASE WHEN CACY.ciudad LIKE 'Puebla%' THEN 'Puebla' 
    ELSE CACY.ciudad END, CASE WHEN R.alias LIKE '%LOMAS%' THEN 'Lomas de Angelopolis' 
    ELSE R.alias END, EXTRACT(MONTH FROM date)
    """
    df_state_mau = client.query(query_ed_estado).to_dataframe()
    return df_state_mau


def parcats():
    query_operaciones = """
    WITH Operaciones AS (
        SELECT CAT.parkingLotName AS proyecto, S.phoneNumber AS user_id, 'ED' AS servicio, TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) AS fecha_operacion
        FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
        JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S ON T.userId = S.userId
        JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_LOT_CAT CAT ON T.parkingLotId = CAT.id
        WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND T.qrCode LIKE "E%"
        UNION ALL
        SELECT ZCA.name AS proyecto, S.phoneNumber AS user_id, 'PV' AS servicio, TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) AS fecha_operacion
        FROM parkimovil-app.cargomovil_pd.PKM_TRANSACTION T
        JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE S ON T.userId = S.userId
        JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_METER_ZONE_CAT ZCA ON T.zoneId = ZCA.id
        WHERE TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND T.paymentType != 1
        UNION ALL
        SELECT R.alias AS proyecto, user AS user_id, 'CA' AS servicio, DATETIME(date) AS fecha_operacion
        FROM parkimovil-app.geosek_raspis.log_sek L
        JOIN `parkimovil-app`.geosek_raspis.raspis R ON L.QR = R.qr
        WHERE idlog IS NOT NULL AND function_ = 'open' AND TIMESTAMP_ADD(date, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00'
    ),
    RankedOperaciones AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY fecha_operacion) AS rank_primera_operacion
        FROM Operaciones
    )

    SELECT
        user_id,
        COUNT(DISTINCT proyecto) > 1 AS multiproyecto,
        COUNT(DISTINCT servicio) > 1 AS multiservicio,
        COUNT(*) AS num_operaciones,
        MAX(CASE WHEN rank_primera_operacion = 1 THEN servicio ELSE NULL END) AS primer_servicio
    FROM RankedOperaciones
    GROUP BY user_id
    """
    df_parcats = client.query(query_operaciones).to_dataframe()
    return df_parcats


df = parcats()
# Definir dimensiones para el Parcats
primer_servicio_dim = go.parcats.Dimension(values=df['primer_servicio'], label="Primer Servicio")
multiproyecto_dim = go.parcats.Dimension(values=df['multiproyecto'], label="Multiproyecto")
multiservicio_dim = go.parcats.Dimension(values=df['multiservicio'], label="Multiservicio")

# Crear escala de colores basada en multiproyecto y multiservicio
colorscale = [[0, 'gray'], [1, 'firebrick']]
color = df['multiproyecto'].astype(int) + df['multiservicio'].astype(int)  # Sumar booleanos convertidos a enteros

# Crear figura de Parcats
fig = go.Figure(data=[go.Parcats(dimensions=[primer_servicio_dim, multiproyecto_dim, multiservicio_dim],
                                 line={'color': color, 'colorscale': colorscale},
                                 hoveron='color', hoverinfo='count+probability',
                                 labelfont={'size': 18, 'family': 'Times'},
                                 tickfont={'size': 16, 'family': 'Times'},
                                 arrangement='freeform')])

fig.update_layout(
    title='Análisis de Usuarios',
    height=800
)


df_state_mau = tree_map_proyects()

fig_tree = px.treemap(df_state_mau[df_state_mau['mes'] == 6], path=['state', 'ciudad', 'proyecto'], values='usuarios',
                 color='usuarios', hover_data=['state', 'ciudad', 'proyecto', 'usuarios'],
                 title='Usuarios por Ciudad y Estado', color_continuous_scale='Viridis')

st.plotly_chart(fig_tree, use_container_width=True)
st.plotly_chart(fig, use_container_width=True)



