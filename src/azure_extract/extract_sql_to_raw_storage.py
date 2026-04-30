from pathlib import Path
from datetime import datetime

import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient

from src.config.settings import (
    SQL_SERVER,
    SQL_DATABASE,
    SQL_USERNAME,
    SQL_PASSWORD,
    STORAGE_CONNECTION_STRING,
    CONTAINER_RAW
)


LOCAL_EXTRACT_PATH = Path("data/local_extracts")


def get_sql_connection():
    connection_string = f"""
    DRIVER={{ODBC Driver 17 for SQL Server}};
    SERVER={SQL_SERVER};
    DATABASE={SQL_DATABASE};
    UID={SQL_USERNAME};
    PWD={SQL_PASSWORD};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
    """
    return pyodbc.connect(connection_string)


def extract_table_to_csv(table_name, conn):
    print(f"Extracting dbo.{table_name} from Azure SQL...")

    query = f"SELECT * FROM dbo.{table_name};"
    df = pd.read_sql(query, conn)

    LOCAL_EXTRACT_PATH.mkdir(parents=True, exist_ok=True)

    file_path = LOCAL_EXTRACT_PATH / f"{table_name}.csv"
    df.to_csv(file_path, index=False)

    print(f"Saved local extract: {file_path}")
    return file_path


def upload_file_to_raw_container(file_path):
    blob_service_client = BlobServiceClient.from_connection_string(
        STORAGE_CONNECTION_STRING
    )

    container_client = blob_service_client.get_container_client(CONTAINER_RAW)

    load_date = datetime.today().strftime("%Y-%m-%d")
    blob_name = f"azure_sql_extract/load_date={load_date}/{file_path.name}"

    print(f"Uploading to raw container as: {blob_name}")

    with open(file_path, "rb") as file:
        container_client.upload_blob(
            name=blob_name,
            data=file,
            overwrite=True
        )

    print("Upload complete.")


tables = [
    "departments",
    "patients",
    "admissions",
    "appointments"
]

conn = get_sql_connection()

for table in tables:
    extracted_file = extract_table_to_csv(table, conn)
    upload_file_to_raw_container(extracted_file)

conn.close()

print("Azure SQL extraction to raw storage completed successfully.")