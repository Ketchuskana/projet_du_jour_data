import streamlit as st
import requests
import time

st.title("Dashboard Projet Data")

# Mesure du temps de refresh des données
start = time.time()

kpis = requests.get("http://localhost:5000/kpis").json()
pays = requests.get("http://localhost:5000/pays").json()
mois = requests.get("http://localhost:5000/mois").json()

end = time.time()
# Fin mesure du temps de refresh des données

st.write("Temps de refresh :", end - start)

st.subheader("KPIs")
st.write(kpis)

st.subheader("Ventes par pays")
st.write(pays)

st.subheader("Ventes par mois")
st.write(mois)
