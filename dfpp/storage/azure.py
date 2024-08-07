import asyncio
import configparser
import logging
import math
import os
import tempfile
from io import BytesIO
from typing import Any, AsyncGenerator, Literal

import pandas as pd
from azure.storage.blob import ContainerClient, ContentSettings
from azure.storage.blob.aio import BlobPrefix
from azure.storage.blob.aio import ContainerClient as AContainerClient

from ..common import cfg2dict, dict2cfg
from ..exceptions import ConfigError
from .utils import *

logger = logging.getLogger(__name__)
TMP_SOURCES = {}


class StorageManager:

    def __init__(self, clear_cache: bool = False):
        self.container_client = AContainerClient.from_container_url(
            container_url=os.environ["AZURE_STORAGE_SAS_URL"]
        )
        self.sync_container_client = ContainerClient.from_container_url(
            container_url=os.environ["AZURE_STORAGE_SAS_URL"]
        )
        self.clear_cache = clear_cache
        self.indicators_cfg_path = "config/indicators"
        self.sources_cfg_path = "config/sources"
        self.utilities_path = "config/utilities"
        self.sources_path = "sources/raw"
        self.output_path = "output"
        self.backup_path = "backup"

    @property
    def container_name(self) -> str:
        return self.container_client.container_name

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __str__(self):
        return (
            f"{self.__class__.__name__}\n"
            f"\t INDICATORS_CFG_PATH: {self.indicators_cfg_path}\n"
            f"\t SOURCES_CFG_PATH: {self.sources_cfg_path}\n"
            f"\t UTILITIES_PATH: {self.utilities_path}\n"
            f"\t SOURCES_PATH: {self.sources_path}\n"
            f"\t OUTPUT_PATH: {self.output_path}\n"
        )

    async def get_md5(self, blob_name: str) -> str:
        """
        :param blob_name:
        :return:
        """
        if not isinstance(blob_name, str):
            raise ValueError("blob_name must be a valid string None")
        elif not await self.check_blob_exists(blob_name):
            raise ValueError(f"Blob {blob_name} does not exist.")

        blob_client = self.container_client.get_blob_client(blob=blob_name)
        properties = await blob_client.get_blob_properties()
        return properties["content_settings"]["content_md5"]

    async def list_configs(
        self, kind: Literal["indicators", "sources"]
    ) -> AsyncGenerator[str, None]:
        path = getattr(self, f"{kind}_cfg_path")
        logger.info(f"Listing {kind} configs at {path}")
        async for blob in self.container_client.list_blobs(name_starts_with=path):
            if isinstance(blob, BlobPrefix):
                continue
            elif not blob.name.endswith(".cfg"):
                continue
            elif kind not in blob.name:
                continue
            yield blob.name

    async def get_indicator_cfg(self, indicator_id: str = None, indicator_path=None):

        try:
            if indicator_id:
                assert (
                    indicator_path is None
                ), f"use either indicator_id or indicator_path"
                indicator_path = (
                    f"{os.path.join(self.indicators_cfg_path, indicator_id)}.cfg"
                )
            else:
                _, indicator_name = os.path.split(indicator_path)
                indicator_id, ext = os.path.splitext(indicator_name)

            assert await self.check_blob_exists(
                indicator_path
            ), f"Indicator {indicator_id} located at {indicator_path} does not exist"

            # TODO caching

            logger.info(
                f"Fetching indicator cfg for {indicator_id} from  {indicator_path}"
            )
            # stream = await self.container_client.download_blob(
            #     indicator_path, max_concurrency=8
            # )
            # content = await stream.readall()

            content = await self.cached_download(source_path=indicator_path)
            content_str = content.decode("utf-8")

            parser = configparser.ConfigParser(interpolation=None)
            parser.read_string(content_str)
            if "indicator" in parser:

                return cfg2dict(parser)

            else:
                raise Exception(
                    f"Indicator  {indicator_id} located at {indicator_path} does not contain an 'indicator' section"
                )
        except Exception as e:
            logger.error(f"Indicator {indicator_id} will be skipped because {e}")
            raise

    async def get_indicators_cfg(
        self, contain_filter: str = None, indicator_ids: list[str] = None
    ):

        tasks = []

        if contain_filter:
            async for blob_name in self.list_configs(kind="indicators"):

                if contain_filter and contain_filter not in blob_name:
                    continue

                t = asyncio.create_task(
                    self.get_indicator_cfg(indicator_path=blob_name)
                )
                tasks.append(t)
        else:
            if indicator_ids is None:
                async for blob_name in self.list_configs(kind="indicators"):
                    t = asyncio.create_task(
                        self.get_indicator_cfg(indicator_path=blob_name)
                    )
                    tasks.append(t)
            else:
                if indicator_ids:
                    for indicator_id in indicator_ids:
                        t = asyncio.create_task(
                            self.get_indicator_cfg(indicator_id=indicator_id)
                        )
                        tasks.append(t)
        results = await asyncio.gather(*tasks)
        return [e for e in results if e]

    async def get_source_cfg(self, source_id=None, source_path=None):
        try:
            if source_id:
                assert source_path is None, f"use either source_id or source_path"
                source_path = f'{os.path.join(self.sources_cfg_path, source_id.lower(), f"{source_id.lower()}.cfg")}'
                _, source_name = os.path.split(source_path)
            else:
                _, source_name = os.path.split(source_path)
                source_id, ext = os.path.splitext(source_name)

            # if 'wbentp1_wb' in indicator_path:raise Exception('forced')
            logger.debug(f"Checking if {source_path} exists")
            assert await self.check_blob_exists(
                source_path
            ), f"Source {source_id} located at {source_path} does not exist"

            logger.info(f"Fetching source cfg  for {source_id} from  {source_path}")
            # stream = await self.container_client.download_blob(
            #     source_path, max_concurrency=8
            # )
            # content = await stream.readall()
            content = await self.cached_download(source_path=source_path)
            logger.debug(f"Downloaded {source_path}")
            content_str = content.decode("utf-8")

            parser = UnescapedConfigParser()
            parser.read_string(content_str)
            logger.debug("Source config read by parser")
            cfg_dict = cfg2dict(parser)
            validate_src_cfg(cfg_dict=cfg_dict["source"])
            return cfg_dict
        except Exception as e:
            logger.error(f"Failed to download {source_id}")
            logger.error(e)
            raise

    async def get_sources_cfgs(self, source_ids: list[str] = None):
        """
        Download and parse source config file of indicators
        :param source_ids:
        :return:
        """
        tasks = []
        if source_ids is None:
            async for blob_name in self.list_configs(kind="sources"):
                source_id = os.path.split(blob_name)[-1].split(".")[0]
                t = asyncio.create_task(self.get_source_cfg(source_id=source_id))
                tasks.append(t)
        else:
            if source_ids:
                for source_id in source_ids:
                    t = asyncio.create_task(self.get_source_cfg(source_id=source_id))
                    tasks.append(t)
        results = await asyncio.gather(*tasks)

        return [e for e in results if e]

    async def close(self):
        await self.container_client.close()
        if self.clear_cache:
            for k, v in TMP_SOURCES.items():
                exists = os.path.exists(v)
                if exists:
                    logger.info(f"Removing cache {v} for source {k} ")
                    os.remove(v)

    async def get_utility_file(self, utility_file: str) -> dict[str, dict[str, Any]]:
        """
        Asynchronously retrieves a specified utility configuration file from the Azure Blob Container,
        parses it, and returns its content as a dictionary.

        Args:
            utility_file (str): The name of the utility configuration file to search for.

        Returns:
            dict: A dictionary representation of the utility configuration file content,
                where keys are section names and values are dictionaries of key-value pairs within the section.

        Raises:
            ConfigError: Raised if the specified utility configuration file is not found or not valid.

        """
        # os.path.join doesn't work for filtering returned Azure blob paths
        async for blob in self.container_client.list_blobs(
            name_starts_with=self.utilities_path
        ):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and utility_file == os.path.basename(blob.name)
            ):
                stream = await self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = await stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser(interpolation=None)
                parser.read_string(content_str)
                config_dict = {
                    section: dict(parser.items(section))
                    for section in parser.sections()
                }
                return config_dict
        else:
            raise ConfigError(f"Utility source not valid")

    async def check_blob_exists(self, blob_name: str) -> bool:
        """
        Checks if a blob exists in the container.
        Args:
            blob_name (str): The name of the blob to check.
        Returns:
            bool: True if the blob exists, False otherwise.
        """
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        return await blob_client.exists()

    async def upload(
        self,
        dst_path: str = None,
        src_path: str = None,
        data: bytes = None,
        content_type: str = None,
        overwrite: bool = True,
    ) -> None:
        """
            Uploads a file or bytes data to Azure Blob Storage.

        async def upload(self, dst_path: str = None, src_path: str = None, content_type=None,
                         overwrite: bool = True) -> None:
            Args:
                dst_path (str, optional): The path of the destination blob in Azure Blob Storage. Defaults to None.
                src_path (str, optional): The local path of the file to upload. Either src_path or data must be provided. Defaults to None.
                data (bytes, optional): The bytes data to upload. Either src_path or data must be provided. Defaults to None.
                content_type (str, optional): The content type of the blob. Defaults to None.
                overwrite (bool, optional): Whether to overwrite the destination blob if it already exists. Defaults to True.

            Raises:
                ValueError: If neither src_path nor data are provided.

            Returns:
                None
        """

        try:

            _, blob_name = os.path.split(dst_path)

            async def _progress_(current, total) -> None:
                print(total)
                progress = current / total * 100
                rounded_progress = int(math.floor(progress))
                logger.info(f"{blob_name} was uploaded - {rounded_progress}%")

            blob_client = self.container_client.get_blob_client(blob=dst_path)
            if src_path:
                with open(src_path, "rb") as f:
                    logger.debug(f"Uploading {src_path} to {dst_path}")
                    await blob_client.upload_blob(
                        data=f,
                        overwrite=overwrite,
                        content_settings=ContentSettings(content_type=content_type),
                        progress_hook=_progress_,
                    )
            elif data:
                logger.debug(f"Uploading bytes/data to {dst_path}")
                await blob_client.upload_blob(
                    data=data,
                    overwrite=overwrite,
                    content_settings=ContentSettings(content_type=content_type),
                    progress_hook=_progress_,
                )
            else:
                raise ValueError("Either 'src_path' or 'data' must be provided.")
        except Exception as e:
            raise e

    def sync_upload(
        self,
        dst_path: str = None,
        src_path: str = None,
        data: bytes = None,
        content_type: str = None,
        overwrite: bool = True,
        max_concurrency: int = 8,
    ):

        try:

            _, blob_name = os.path.split(dst_path)

            def _progress_(current, total) -> None:
                progress = current / total * 100
                rounded_progress = int(math.floor(progress))
                logger.info(f"{blob_name} was uploaded - {rounded_progress}%")

            blob_client = self.sync_container_client.get_blob_client(blob=dst_path)
            if src_path:
                with open(src_path, "rb") as f:
                    logger.debug(f"Uploading {src_path} to {dst_path}")
                    blob_client.upload_blob(
                        data=f,
                        overwrite=overwrite,
                        content_settings=ContentSettings(content_type=content_type),
                        progress_hook=_progress_,
                        max_concurrency=max_concurrency,
                    )
            elif data:
                logger.debug(f"Uploading bytes/data to {dst_path}")
                blob_client.upload_blob(
                    data=data,
                    overwrite=overwrite,
                    content_settings=ContentSettings(content_type=content_type),
                    progress_hook=_progress_,
                    max_concurrency=max_concurrency,
                )
            else:
                raise ValueError("Either 'src_path' or 'data' must be provided.")
        except Exception as e:
            raise e

    async def download(
        self,
        blob_name: str = None,
        dst_path: str = None,
    ) -> bytes | None:
        """
        Downloads a file from Azure Blob Storage and returns its data or saves it to a local file.

        Args:
            blob_name (str, optional): The name of the blob to download. Defaults to None.
            dst_path (str, optional): The local path to save the downloaded file. If not provided, the file data is returned instead of being saved to a file. Defaults to None.

        Returns:
            bytes or None: The data of the downloaded file, or None if a dst_path argument is provided.
        """
        # _, b_name = os.path.split(blob_name)
        #
        # async def _progress_(current, total) -> None:
        #     progress = current / total * 100
        #     rounded_progress = int(math.floor(progress))
        #     logger.info(f'{b_name} was downloaded - {rounded_progress}%')
        try:
            logger.debug(f"Downloading {blob_name}")
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            chunk_list = []
            stream = await blob_client.download_blob()
            async for chunk in stream.chunks():
                chunk_list.append(chunk)

            data = b"".join(chunk_list)
            logger.debug(f"Finished downloading {blob_name}")
            if dst_path:
                with open(dst_path, "wb") as f:
                    f.write(data)
                return None
            else:
                return data
        except Exception as e:
            print(e)

    async def delete_blob(self, blob_path):
        """
        :param blob_path:
        :return:
        """
        return await self.container_client.delete_blob(blob_path)

    async def copy_blob(self, source_blob_name: str, destination_blob_name: str):
        """
        Copy a blob from a source to a destination
        :param source_blob_name:
        :param destination_blob_name:
        :return:
        """
        source_blob = self.container_client.get_blob_client(blob=source_blob_name)
        destination_blob = self.container_client.get_blob_client(
            blob=destination_blob_name
        )
        return await destination_blob.start_copy_from_url(source_blob.url)

    async def list_base_files(self, indicator_ids=None):
        """
        List
        :param indicator_id:
        :return:
        """
        return [
            blob.name
            async for blob in self.container_client.list_blobs(
                name_starts_with=os.path.join(
                    self.output_path, "access_all_data", "base/"
                )
            )
        ]

    async def upload_cfg(self, cfg_dict=None, cfg_path=None):
        """
        Upload a config file to azure storage
        :param cfg_dict:
        :param cfg_path:
        :return:
        """
        parser = dict2cfg(cfg_dict=cfg_dict)
        content = parser.write_string()
        content_bytes = content.encode("utf-8")
        await self.upload(
            dst_path=cfg_path, data=content_bytes, content_type="text/plain"
        )

    async def list_blobs(self, prefix=None):
        """
        List
        :param prefix:
        :return:
        """
        return [
            blob.name
            async for blob in self.container_client.list_blobs(name_starts_with=prefix)
        ]

    async def cached_download(self, source_path=None, chunked=False):
        logger.debug(f"Downloading {source_path}")
        if source_path in TMP_SOURCES:
            cached_src_path = TMP_SOURCES[source_path]
            logger.info(f"Rereading cached {source_path} from {cached_src_path} ")
            data = open(cached_src_path, "rb").read()
        else:

            # check the blob exists in azure storage
            source_path_exists = await self.check_blob_exists(blob_name=source_path)
            logger.debug(f"Checking if {source_path} exists {source_path_exists}")
            if not source_path_exists:
                raise Exception(f"Source {source_path} does not exist")
            stream = await self.container_client.download_blob(
                source_path, max_concurrency=1
            )
            logger.debug(f"Downloaded {source_path}")
            if chunked:
                chunk_list = []
                async for chunk in stream.chunks():
                    logger.debug(f"Downloaded {len(chunk)}")
                    chunk_list.append(chunk)
                data = b"".join(chunk_list)
            else:
                data = await stream.readall()

            cached = tempfile.NamedTemporaryFile(mode="wb", delete=False)
            logger.debug(f"Going to cache {source_path} to {cached.name}")
            cached.write(data)
            TMP_SOURCES[source_path] = cached.name
        return data

    async def get_lookup_df(
        self, sheet: Literal["country", "region", "country_code"]
    ) -> pd.DataFrame:
        path = f"{self.utilities_path}/country_lookup.xlsx"
        data = await self.cached_download(source_path=path)
        df = pd.read_excel(BytesIO(data), sheet_name=f"{sheet}_lookup")
        return df
