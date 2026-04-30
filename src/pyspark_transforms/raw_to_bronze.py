from pathlib import Path
import shutil

from pyspark.sql import SparkSession


RAW_PATH = Path("data/local_extracts")
BRONZE_PATH = Path("data/bronze")

tables = [
    "departments",
    "patients",
    "admissions",
    "appointments"
]


spark = (
    SparkSession.builder
    .appName("NHS Bronze Layer")
    .master("local[*]")
    .getOrCreate()
)

print("Spark session started.")


for table in tables:
    input_file = str(RAW_PATH / f"{table}.csv")
    output_folder = BRONZE_PATH / table

    print(f"Reading {input_file}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_file)
    )

    print(f"Rows in {table}: {df.count()}")

    if output_folder.exists():
        shutil.rmtree(output_folder)

    output_folder.mkdir(parents=True, exist_ok=True)

    pandas_df = df.toPandas()
    pandas_df.to_parquet(output_folder / f"{table}.parquet", index=False)

    print(f"Written Bronze dataset: {output_folder}")


spark.stop()
print("Bronze layer complete.")