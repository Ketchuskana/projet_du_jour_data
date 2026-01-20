import streamlit as st
import requests
import time

st.title("Dashboard Projet Data")

api_url = "http://api:5000"

# Attendre que l'API Flask soit disponible
max_retries = 10
for i in range(max_retries):
    try:
        kpis = requests.get(f"{api_url}/kpis").json()
        pays = requests.get(f"{api_url}/pays").json()
        mois = requests.get(f"{api_url}/mois").json()
        break
    except requests.exceptions.ConnectionError:
        st.write(f"API non disponible, tentative {i+1}/{max_retries}...")
        time.sleep(2)
else:
    st.error("Impossible de se connecter à l'API Flask !")

# Affichage des données
if 'kpis' in locals():
    start = time.time()
    end = time.time()
    
    st.write("Temps de refresh :", end - start)
    
    st.subheader("KPIs")
    st.write(kpis)
    
    st.subheader("Ventes par pays")
    st.write(pays)
    
    st.subheader("Ventes par mois")
    st.write(mois)
