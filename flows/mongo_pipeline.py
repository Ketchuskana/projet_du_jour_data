import pandas as pd
from pymongo import MongoClient

def run():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["projet_data"]

    # Lecture des fichiers Parquet
    df_kpis = pd.read_parquet("data/gold/kpis_global.parquet")
    df_pays = pd.read_parquet("data/gold/ventes_par_pays.parquet")
    df_mois = pd.read_parquet("data/gold/ventes_par_mois.parquet")

    # Vider les anciennes collections et insérer les nouvelles
    db.kpis.delete_many({})
    db.kpis.insert_many(df_kpis.to_dict(orient="records"))

    db.ventes_par_pays.delete_many({})
    db.ventes_par_pays.insert_many(df_pays.to_dict(orient="records"))

    db.ventes_par_mois.delete_many({})
    db.ventes_par_mois.insert_many(df_mois.to_dict(orient="records"))

    print("Données insérées dans MongoDB avec succès !")

if __name__ == "__main__":
    run()
