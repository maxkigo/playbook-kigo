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
    page_title="Kigo - Zonas Metropolitanas",
    layout="wide"
)

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)




def zm_playbook(zona_metropolitana):

    mau_ed = f"""
    SELECT COUNT(DISTINCT P.phoneNumber) AS MAU, EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR)) AS mes
    FROM parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    JOIN parkimovil-app.cargomovil_pd.SEC_USER_PROFILE P 
        ON T.userId = P.userId
    JOIN parkimovil-app.cargomovil_pd.PKM_PARKING_LOT_CAT CAT
        ON T.parkinglotId = CAT.id
    JOIN parkimovil-app.cargomovil_pd.GEN_CITY_CAT C
        ON CAT.cityId = C.id
    JOIN parkimovil-app.cargomovil_pd.zm_playbook Z
        ON C.name = Z.string_field_0
    WHERE TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR) >= '2024-01-01 00:00:00' AND Z.string_field_3 LIKE '{zona_metropolitana}'
    GROUP BY EXTRACT(MONTH FROM TIMESTAMP_ADD(paymentDate, INTERVAL - 6 HOUR))
    ORDER BY mes
    """

    df_multiservicio_gen = client.query(mau_ed).to_dataframe()
    return df_multiservicio_gen

zona_metropolinata = ['ZM Puebla', 'ZM Monterrey']
zona_metropolinata_seleccionada = st.selectbox('Selecciona una Zona Metropolitana:', zona_metropolinata)

df_mau_general = zm_playbook(zona_metropolinata_seleccionada)

st.write(df_mau_general)