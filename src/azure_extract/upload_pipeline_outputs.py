from pathlib import Path
from azure.storage.blob import BlobServiceClient

from src.config.settings import (
    STORAGE_CONNECTION_STRING,
    CONTAINER_BRONZE,
    CONTAINER_SILVER,
    CONTAINER_GOLD
)


blob_service_client = BlobServiceClient.from_connection_string(
    STORAGE_CONNECTION_STRING
)


def upload_folder(local_root, container_name):
    local_root = Path(local_root)

    container_client = blob_service_client.get_container_client(container_name)

    for file in local_root.rglob("*"):
        if file.is_file():
            blob_name = file.relative_to(local_root).as_posix()

            print(f"Uploading {file} -> {container_name}/{blob_name}")

            with open(file, "rb") as data:
                container_client.upload_blob(
                    name=blob_name,
                    data=data,
                    overwrite=True
                )


upload_folder("data/bronze", CONTAINER_BRONZE)
upload_folder("data/silver", CONTAINER_SILVER)
upload_folder("data/gold", CONTAINER_GOLD)

print("All pipeline outputs uploaded successfully.")