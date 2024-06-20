import asyncio
import configparser
import json

from azure.storage.blob.aio import BlobPrefix, ContainerClient

from dfpp.dfpp_exceptions import ConfigError


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


async def list(connection_string: str = None, container_name=None, prefix: str = None):
    cfg = {}

    async with ContainerClient.from_connection_string(
            conn_str=connection_string, container_name=container_name
    ) as cc:
        async for blob in cc.walk_blobs(name_starts_with=prefix, delimiter=""):
            # if isinstance(blob, BlobPrefix) and not (blob.name.endswith(prefix) or 'indicators' in blob.name):
            if (
                    not isinstance(blob, BlobPrefix)
                    and blob.name.endswith(".cfg")
                    and not "indicators" in blob.name
            ):
                print(blob.name)
                stream = await cc.download_blob(blob.name, max_concurrency=8)
                content = await stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if "source" in parser:
                    src_id = parser["source"].get("id")
                    print(src_id in blob.name, "in source folder")
                    cfg[src_id] = dict(parser["source"].items())

                else:
                    raise ConfigError(f"Invalid source")
                # if 'indicator' in parser:
                #     print(parser.options('indicator'))

    print(json.dumps(cfg, indent=2))


if __name__ == "__main__":
    import os
    from constants import *

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    root_folder = os.getenv("ROOT_FOLDER")
    account_name = connection_string.split(";")[1].split("=")[1]
    prefix = f'{os.path.join(root_folder, "config", "sources")}'
    asyncio.run(
        list(
            connection_string=connection_string,
            container_name=container_name,
            prefix=prefix,
        )
    )
