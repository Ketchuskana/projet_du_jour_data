import os
from minio import Minio

# === Configuration Prefect ===
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

# === Buckets MinIO ===
BUCKET_SOURCES = "sources"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

# === Configuration MinIO ===
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = False


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


def configure_prefect() -> None:
    os.environ["PREFECT_API_URL"] = PREFECT_API_URL


if __name__ == "__main__":
    client = get_minio_client()
    print("MinIO client ready:", client)

    configure_prefect()
    print("Prefect API URL:", os.getenv("PREFECT_API_URL"))
