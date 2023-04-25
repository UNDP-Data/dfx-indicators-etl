import os
import aiohttp
import ast
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import ContentSettings
import azblob
import logging


CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']
CONTAINER_NAME = os.environ['CONTAINER_NAME']
ROOT_FOLDER = os.environ['ROOT_FOLDER']


async def download_sources():
    async with BlobServiceClient.from_connection_string(CONNECTION_STRING) as blob_service_client:
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        sources = await azblob.list_blobs(
            connection_string=CONNECTION_STRING,
            container_name=CONTAINER_NAME,
            prefix=os.path.join(ROOT_FOLDER, 'pipeline', 'config', 'sources')
        )

        async def download(source_id, source_config, save_as):
            """
            :param source_id: The source id, derived from the source config file name.
            :param source_config: The source config dictionary. derived from the source config file.
            :param save_as: The name of the file to save the downloaded data blob as.
            :return: None if successful, otherwise an error message.
            """
            url = source_config['url']
            blob_name = os.path.join(ROOT_FOLDER, 'pipeline', 'data_sources', f'{save_as}')
            if 'params' in source_config:
                params = ast.literal_eval(source_config['params'])
            else:
                params = {}
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            print(response.headers.get('content-type'))
                            content_settings = ContentSettings(content_type=response.headers.get('content-type'))
                            data = await response.read()
                            await container_client.upload_blob(blob_name, data, overwrite=True, max_concurrency=8,
                                                               content_settings=content_settings)
                            print(f"{source_id} downloaded and uploaded successfully.")
                        else:
                            print(f"Failed to download {source_id}: {response.status}")
            except asyncio.TimeoutError:
                print(f"Timeout error occurred while downloading {source_id}.")
            except aiohttp.ClientError as e:
                print(f"Client error occurred while downloading {source_id}: {e}")
            except Exception as e:
                print(f"Error occurred while downloading {source_id}: {e}")
        tasks = []
        for sid, config in sources.items():  # sid = source id, config = source config
            task = asyncio.create_task(download(sid, config, config['save_as']))
            tasks.append(task)
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    import asyncio
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    asyncio.run(download_sources())
