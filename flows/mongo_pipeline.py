import pandas as pd
from pymongo import MongoClient

def load_csv(name):
    return pd.read_csv(f"data/gold/{name}")

def send_to_mongo(df, collection_name):

    client = MongoClient("mongodb://localhost:27017/")
    db = client["projet_data"]

    print(db.list_collection_names())

    collection = db[collection_name]

    # Vider l’ancienne collection
    collection.delete_many({})

    # insertion des nouvelles données
    collection.insert_many(df.to_dict("records"))

    print(f"Données insérées dans {collection_name}")


def run():

    kpis = load_csv("kpis_global.csv")
    pays = load_csv("ventes_par_pays.csv")
    mois = load_csv("ventes_par_mois.csv")

    send_to_mongo(kpis, "kpis")
    send_to_mongo(pays, "ventes_par_pays")
    send_to_mongo(mois, "ventes_par_mois")


if __name__ == "__main__":
    run()
