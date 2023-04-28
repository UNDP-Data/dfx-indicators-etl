import os
import configparser
import asyncio
from azure.storage.blob.aio import ContainerClient, BlobPrefix
import json


class ConfigError(Exception):
    pass


# CONSTANTS
CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']
CONTAINER_NAME = os.environ['CONTAINER_NAME']
ROOT_FOLDER = os.environ['ROOT_FOLDER']


async def list_blobs(connection_string: str = None, container_name=None, prefix: str = None):
    sources_cfg = {}
    async with ContainerClient.from_connection_string(conn_str=connection_string, container_name=container_name) as cc:
        async for blob in cc.walk_blobs(name_starts_with=prefix, delimiter=''):
            if not isinstance(blob, BlobPrefix) and blob.name.endswith('.cfg') and not 'indicators' in blob.name:
                stream = await cc.download_blob(blob.name, max_concurrency=8)
                content = await stream.readall()
                content_str = content.decode('utf-8')
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if 'source' in parser:
                    src_id = parser['source'].get('id')
                    sources_cfg[src_id] = dict(parser['source'].items())
                    if 'downloader_params' in parser:
                        sources_cfg[src_id]['downloader_params'] = dict(parser['downloader_params'].items())
                else:
                    raise ConfigError(f'Invalid source')
    return sources_cfg


async def list_indicators():
    prefix = os.path.join(ROOT_FOLDER, 'pipeline', 'config', 'sources')
    sources = await list_blobs(connection_string=CONNECTION_STRING, container_name=CONTAINER_NAME, prefix=prefix)
    print(sources.keys())

if __name__ == '__main__':
    # import os
    # account_name = CONNECTION_STRING.split(';')[1].split('=')[1]
    # prefix = f'{os.path.join(ROOT_FOLDER, "pipeline", "config", "sources")}'
    # asyncio.run(list_blobs(connection_string=CONNECTION_STRING, container_name=CONTAINER_NAME, prefix=prefix))
    asyncio.run(list_indicators())
