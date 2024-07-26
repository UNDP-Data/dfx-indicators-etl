import asyncio
import os

import pandas as pd

from .storage import AsyncAzureBlobStorageManager


def add_preprocessing_field_to_sources_list_local():
    df = pd.read_csv('./indicator_list.csv')
    df['preprocessing'] = df['Transform Notebook'].apply(lambda x: x.lower() + '_preprocessing')
    os.makedirs('dict2cfg', exist_ok=True)
    df.to_csv('./dict2cfg/indicator_list.csv', index=False)


async def upload_missing_manual_files_from_old_pipeline():
    source_folder_path = "DataFuturePlatform/Sources/Raw/"
    destination_folder_path = "DataFuturePlatform/pipeline/sources/raw/"
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
        container_name=os.getenv('CONTAINER_NAME'),
    )
    source_blobs = await storage.list_blobs(prefix=source_folder_path)
    destination_blobs = await storage.list_blobs(prefix=destination_folder_path)
    blobs_to_copy = [blob for blob in source_blobs if blob.name.split("/")[-1] not in [destination_blob.name.split("/")[-1] for destination_blob in destination_blobs]]
    print('Blobs to copy', len(blobs_to_copy))
    for source_blob in blobs_to_copy:
        destination_blob = destination_folder_path + source_blob.name.split('/')[-1]
        await storage.copy_blob(source_blob.name, destination_blob)
    await storage.close()


if __name__ == "__main__":
    asyncio.run(upload_missing_manual_files_from_old_pipeline())
