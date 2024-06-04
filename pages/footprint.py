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

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

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
mau_last_month = df_mau_general['MAU'].iloc[-3]
mau_actual = df_mau_general['MAU'].iloc[-2]
