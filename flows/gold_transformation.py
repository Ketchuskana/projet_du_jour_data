from io import BytesIO
import pandas as pd
from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


@task
def load_silver(object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_SILVER, object_name)

    df = pd.read_csv(response)
    response.close()
    response.release_conn()

    return df


@task
def kpis_global(clients: pd.DataFrame, achats: pd.DataFrame) -> pd.DataFrame:

    total_clients = clients["id_client"].nunique()
    total_achats = len(achats)
    ca_total = achats["montant"].sum()

    df = pd.DataFrame([{
        "total_clients": total_clients,
        "total_achats": total_achats,
        "chiffre_affaires": ca_total
    }])

    return df


@task
def ventes_par_pays(clients, achats):

    merged = achats.merge(clients, on="id_client")

    result = merged.groupby("pays").agg({
        "montant": "sum",
        "id_achat": "count"
    }).rename(columns={
        "montant": "CA",
        "id_achat": "nb_ventes"
    }).reset_index()

    return result


@task
def ventes_temporelles(achats):

    achats["date_achat"] = pd.to_datetime(achats["date_achat"])

    achats["mois"] = achats["date_achat"].dt.to_period("M")

    result = achats.groupby("mois").agg({
        "montant": "sum"
    }).rename(columns={"montant": "CA"}).reset_index()

    return result


@task
def save_gold(df: pd.DataFrame, name: str):

    client = get_minio_client()

    csv = df.to_csv(index=False).encode()

    client.put_object(
        BUCKET_GOLD,
        name,
        BytesIO(csv),
        length=len(csv)
    )

    print(f"{name} saved to GOLD")


@flow(name="Gold Transformation Flow")
def gold_flow():

    clients = load_silver("clients.csv")
    achats = load_silver("achats.csv")

    kpi = kpis_global(clients, achats)
    pays = ventes_par_pays(clients, achats)
    temps = ventes_temporelles(achats)

    save_gold(kpi, "kpis_global.csv")
    save_gold(pays, "ventes_par_pays.csv")
    save_gold(temps, "ventes_par_mois.csv")


if __name__ == "__main__":
    gold_flow()
