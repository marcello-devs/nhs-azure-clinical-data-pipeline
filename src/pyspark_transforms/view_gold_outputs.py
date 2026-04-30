from pathlib import Path
import pandas as pd


GOLD_PATH = Path("data/gold")

tables = [
    "admission_kpis",
    "waiting_list_kpis",
    "patient_demographics"
]


for table in tables:
    print("\n" + "=" * 80)
    print(table.upper())
    print("=" * 80)

    file_path = GOLD_PATH / table / f"{table}.parquet"

    df = pd.read_parquet(file_path)

    print(df.head(20))