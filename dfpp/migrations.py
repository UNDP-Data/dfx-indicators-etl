import asyncio
import configparser
import io
import os

import pandas as pd

from dfpp.storage import AsyncAzureBlobStorageManager


async def source_list_csv():
    storage_manager = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        container_name=os.getenv("CONTAINER_NAME"),
    )
    source_csv = await storage_manager.download(
        blob_name="DataFuturePlatform/Sources/source_list.csv"
    )
    source_csv_io = io.BytesIO(source_csv)
    await storage_manager.close()
    return source_csv_io


async def create_config():
    sources_io = await source_list_csv()
    sources_df = pd.read_csv(sources_io)
    parser = configparser.ConfigParser()
    for row in sources_df.iterrows():
        parser['source'] = {
            'id': row[1]['Source Id'],
            'name': row[1]['Source Name'],
            'url': row[1]['Source URL'],
            'frequency': row[1]['Frequency'],
        }
        # write the config to a local file



async def main():
    pass


if __name__ == '__main__':
    asyncio.run(main())
