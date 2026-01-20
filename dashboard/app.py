import streamlit as st
import requests
import time

st.title("Dashboard Projet Data")

API_URL = "http://api:5000"

start = time.time()

try:
    kpis = requests.get(f"{API_URL}/kpis").json()
    pays = requests.get(f"{API_URL}/pays").json()
    mois = requests.get(f"{API_URL}/mois").json()

    end = time.time()

    st.info(f"Temps total de refresh : {round(end - start, 3)} secondes")

    st.subheader("KPIs")
    st.write(kpis)

    st.subheader("Ventes par pays")
    st.write(pays)

    st.subheader("Ventes par mois")
    st.write(mois)

except Exception as e:
    st.error("Impossible de se connecter à l'API Flask !")
    st.write(e)
