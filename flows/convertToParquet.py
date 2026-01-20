import pandas as pd
import pathlib

# dossier gold
gold_path = pathlib.Path("data/gold")

# pour chaque CSV dans le dossier
for csv_file in gold_path.glob("*.csv"):
    df = pd.read_csv(csv_file)
    parquet_file = csv_file.with_suffix(".parquet") 
    df.to_parquet(parquet_file, index=False)
    print(f"Converti {csv_file.name} → {parquet_file.name}")
