import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

def generate_achats(client_ids, n_achats: int, output_path: str):
    produits = ["Laptop", "Phone", "Tablet", "Headphones", "Camera", "Monitor"]
    achats = []

    for i in range(1, n_achats + 1):
        client_id = random.choice(client_ids)
        date_achat = datetime.now() - timedelta(days=random.randint(0, 365))
        montant = round(random.uniform(20, 2000), 2)
        achats.append({
            "id_achat": i,
            "id_client": client_id,
            "produit": random.choice(produits),
            "montant": montant,
            "date_achat": date_achat.strftime("%Y-%m-%d")
        })

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id_achat", "id_client", "produit", "montant", "date_achat"])
        writer.writeheader()
        writer.writerows(achats)

    print(f"Generated {n_achats} achats in file {output_path}")

if __name__ == "__main__":
    import sys
    client_ids = list(range(1, 1501))  # IDs générés par generate_clients
    output_dir = Path(__file__).parent.parent / "data" / "sources"
    generate_achats(client_ids, 3000, str(output_dir / "achats.csv"))
