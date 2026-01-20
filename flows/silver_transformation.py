from io import BytesIO
from prefect import flow, task
from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client

import pandas as pd


@task
def load_csv_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(bucket, object_name)

    df = pd.read_csv(response)
    response.close()
    response.release_conn()

    return df


@task
def clean_clients(df: pd.DataFrame) -> pd.DataFrame:

    # Suppression des doublons
    df = df.drop_duplicates(subset=["id_client"])

    # Valeurs manquantes
    df = df.dropna(subset=["email", "nom"])

    # Format de date standardisé
    df["date_inscription"] = pd.to_datetime(df["date_inscription"])

    # Normalisation
    df["pays"] = df["pays"].str.title()

    return df


@task
def clean_achats(df: pd.DataFrame) -> pd.DataFrame:

    # Suppression doublons
    df = df.drop_duplicates()

    # Suppression lignes invalides
    df = df.dropna(subset=["id_client", "montant"])

    # Normalisation types
    df["date_achat"] = pd.to_datetime(df["date_achat"])
    df["montant"] = df["montant"].astype(float)

    # Valeurs aberrantes
    df = df[df["montant"] > 0]

    return df


@task
def save_to_silver(df: pd.DataFrame, object_name: str):

    client = get_minio_client()

    csv_bytes = df.to_csv(index=False).encode("utf-8")

    client.put_object(
        BUCKET_SILVER,
        object_name,
        BytesIO(csv_bytes),
        length=len(csv_bytes)
    )

    print(f"{object_name} saved to SILVER layer")


@flow(name="Silver Transformation Flow")
def silver_flow():

    clients = load_csv_from_minio(BUCKET_BRONZE, "clients.csv")
    achats = load_csv_from_minio(BUCKET_BRONZE, "achats.csv")

    clients_clean = clean_clients(clients)
    achats_clean = clean_achats(achats)

    save_to_silver(clients_clean, "clients.csv")
    save_to_silver(achats_clean, "achats.csv")


if __name__ == "__main__":
    silver_flow()
