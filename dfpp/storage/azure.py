import asyncio
import configparser
import logging
import math
import os
from io import BytesIO
from typing import Any, Literal

import pandas as pd
from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import BlobPrefix, ContainerClient

from dfpp.common import cfg2dict
from dfpp.exceptions import ConfigError
from dfpp.storage.utils import *
from dfpp.data_models import Source, Indicator

logger = logging.getLogger(__name__)


class StorageManager:

    def __init__(self, clear_cache: bool = False):
        self.container_client = ContainerClient.from_container_url(
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

    async def get_md5(self, path: str) -> str:
        """
        Get MD5 hash of a blob.

        Parameters
        ----------
        path : str
            Path to a blob.

        Returns
        -------
        str
            MD5 hash of the blob.
        """
        if not isinstance(path, str):
            raise ValueError("blob_name must be a valid string None")
        blob_client = self.container_client.get_blob_client(blob=path)
        if not await blob_client.exists():
            raise ValueError(f"Blob at {path} does not exist.")
        properties = await blob_client.get_blob_properties()
        return properties["content_settings"]["content_md5"]

    async def list_configs(self, kind: Literal["indicators", "sources"]) -> list[str]:
        path = getattr(self, f"{kind}_cfg_path")
        logger.info(f"Listing {kind} configs at {path}")
        blob_names = []
        async for blob in self.container_client.list_blobs(name_starts_with=path):
            if isinstance(blob, BlobPrefix):
                continue
            elif not blob.name.endswith(".cfg"):
                continue
            blob_names.append(blob.name)
        return blob_names

    async def get_indicator_cfg(self, indicator_id_or_path: str):
        if not isinstance(indicator_id_or_path, str):
            raise ValueError("indicator_id_or_path must be a valid string")

        if indicator_id_or_path.endswith(".cfg"):
            *_, indicator_name = os.path.split(indicator_id_or_path)
            indicator_id, _ = os.path.splitext(indicator_name)
            indicator_path = indicator_id_or_path
        else:
            indicator_id = indicator_id_or_path
            indicator_path = (
                f"{os.path.join(self.indicators_cfg_path, indicator_id)}.cfg"
            )

        try:
            if not await self.check_blob_exists(indicator_path):
                raise ValueError(f"Blob {indicator_path} does not exist.")
            logger.info(
                f"Fetching indicator cfg for {indicator_id} from  {indicator_path}"
            )

            content = await self.read_blob(path=indicator_path)
            content_str = content.decode("utf-8")

            parser = UnescapedConfigParser(interpolation=None)
            parser.read_string(content_str)
            if "indicator" in parser:
                 indicator_cfg_dict = Indicator.flatten_dict_config(cfg2dict(parser))
                 return dict(Indicator(**indicator_cfg_dict))
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
        if indicator_ids is None:
            indicator_ids = await self.list_configs(kind="indicators")
        tasks = []
        for indicator in indicator_ids:
            if contain_filter is not None and contain_filter not in indicator:
                continue
            t = asyncio.create_task(
                self.get_indicator_cfg(indicator_id_or_path=indicator)
            )
            tasks.append(t)
        results = await asyncio.gather(*tasks)
        return [e for e in results if e]

    async def get_source_cfg(self, source_id_or_path: str):
        if not isinstance(source_id_or_path, str):
            raise ValueError("source_id_or_path must be a valid string")

        if source_id_or_path.endswith(".cfg"):
            *_, source_name = os.path.split(source_id_or_path)
            source_id, _ = os.path.splitext(source_name)
            source_path = source_id_or_path
        else:
            source_id = source_id_or_path
            source_path = f"{os.path.join(self.sources_cfg_path, source_id.lower(), source_id.lower())}.cfg"

        try:
            logger.debug(f"Checking if {source_path} exists")
            if not await self.check_blob_exists(source_path):
                raise ValueError(f"Source {source_id} at {source_path} does not exist")
            logger.info(f"Fetching source cfg  for {source_id} from  {source_path}")
            content = await self.read_blob(path=source_path)
            logger.debug(f"Downloaded {source_path}")
            content_str = content.decode("utf-8")

            parser = UnescapedConfigParser()
            parser.read_string(content_str)
            logger.debug("Source config read by parser")
            cfg_dict = cfg2dict(parser)
            source_cfg_dict = Source.flatten_dict_config(cfg_dict)
            return dict(Source(**source_cfg_dict))
        except Exception as e:
            logger.error(f"Failed to download {source_id}")
            logger.error(e)
            raise

    async def get_sources_cfgs(self, source_ids: list[str] = None):
        if source_ids is None:
            sources = await self.list_configs(kind="sources")
        else:
            sources = source_ids
        tasks = []
        for source in sources:
            t = asyncio.create_task(self.get_source_cfg(source_id_or_path=source))
            tasks.append(t)
        results = await asyncio.gather(*tasks)
        return [e for e in results if e]

    async def close(self):
        await self.container_client.close()

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

    async def upload_blob(
        self,
        path_or_data_src: str | bytes,
        path_dst: str,
        content_type: str | None = None,
        overwrite: bool = True,
    ) -> str:
        """
        Uploads a file to Azure Blob Container.

        Parameters
        ----------
        path_or_data_src : str | bytes
            Source path or data to upload.
        path_dst : str
            Destination path to upload to.
        content_type : str, optional
            Content data type.
        overwrite : bool, default=True
            Flag to overwrite existing file if it exists.

        Returns
        -------
        path_dst : str
            Destination path the blob was uploaded to.
        """
        if isinstance(path_or_data_src, bytes):
            data = path_or_data_src
        elif isinstance(path_or_data_src, str):
            with open(path_or_data_src, "rb") as file:
                data = file.read()
        else:
            raise ValueError("src_path_data must be bytes or str")

        async def _progress_(current: int, total: int) -> None:
            print(total)
            progress = current / total * 100
            rounded_progress = int(math.floor(progress))
            logger.info(f"{path_dst} was uploaded - {rounded_progress}%")

        try:
            blob_client = self.container_client.get_blob_client(blob=path_dst)
            await blob_client.upload_blob(
                data=data,
                overwrite=overwrite,
                content_settings=ContentSettings(content_type=content_type),
                progress_hook=_progress_,
            )
            logger.debug(f"Uploading bytes/data to {path_dst}")
        except Exception as e:
            logger.error(f"Failed to upload the blob to {path_dst}: {e}")
        else:
            return path_dst

    async def read_blob(self, path: str) -> bytes:
        """
        Read a blob from Azure Blob Storage.

        Parameters
        ----------
        path : str
            Path to the blob to read.

        Returns
        -------
        data : bytes
            Blob data as bytes.
        """
        logger.debug(f"Checking in the blob at {path} exists")
        blob_client = self.container_client.get_blob_client(blob=path)
        if not await blob_client.exists():
            raise ValueError(f"Blob at {path} does not exist")

        try:
            logger.debug(f"Reading the blob at {path}")
            blob = await blob_client.download_blob()
            data = await blob.readall()
        except Exception as e:
            logger.error(f"Failed to read the blob at {path}: {e}")
        else:
            return data

    async def download_blob(self, path_src: str, path_dst: str | None = None) -> str:
        """
        Download a blob from Azure Blob Storage to a local storage.

        Parameters
        ----------
        path_src : str
            Source path to download the blob from.
        path_dst : str, optional
            Destination path to save the blob to. If not provided, saves to the blob using its original name
            to the current directory.

        Returns
        -------
        path_dst : str
            Destination path the blob was downloaded to.
        """
        logger.debug(f"Checking in the blob at {path_src} exists")
        blob_client = self.container_client.get_blob_client(blob=path_src)
        if not await blob_client.exists():
            raise ValueError(f"Blob at {path_src} does not exist")

        if isinstance(path_dst, str):
            pass
        elif path_dst is None:
            path_dst = blob_client.blob_name
        else:
            raise ValueError(
                f"path_dst must be either None or a valid string, not {type(path_dst)}"
            )

        try:
            logger.debug(f"Downloading the blob from {path_dst}")
            blob = await blob_client.download_blob()
            async for chunk in blob.chunks():
                with open(path_dst, "wb") as f:
                    f.write(chunk)
        except Exception as e:
            logger.error(f"Failed to download the blob from {path_src}: {e}")
        else:
            logger.debug(f"Downloaded blob {path_src} to {path_dst}")
            return path_dst

    async def delete_blob(self, path: str) -> None:
        """
        Delete a blob from Azure Blob Storage.

        Parameters
        ----------
        path : str
            Path to the blob to delete.

        Returns
        -------
        None.
        """
        try:
            await self.container_client.delete_blob(blob=path, timeout=10)
        except Exception as e:
            logger.error(f"Failed to delete the blob from {path}: {e}")

    async def copy_blob(self, path_src: str, path_dst: str) -> bool:
        """
        Copy a blob within Azure Blob Storage.

        Parameters
        ----------
        path_src : str
            Source path to copy the blob from.
        path_dst : str, optional
            Destination path to copy the blob to.

        Returns
        -------
        bool
            True, if the blob was copied successfully. False, otherwise.
        """
        blob_client_src = self.container_client.get_blob_client(blob=path_src)
        if not await blob_client_src.exists():
            raise ValueError(f"Blob at {path_src} does not exist")
        blob_client_dst = self.container_client.get_blob_client(blob=path_dst)
        await blob_client_dst.start_copy_from_url(blob_client_src.url)
        return await blob_client_dst.exists()

    async def list_blobs(self, prefix: str | None = None):
        """
        List
        :param prefix:
        :return:
        """
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        return [blob.name async for blob in blobs]

    async def list_base_files(self) -> list[str]:
        prefix = os.path.join(self.output_path, "access_all_data", "base/")
        return await self.list_blobs(prefix)

    async def get_lookup_df(
        self, sheet: Literal["country", "region", "country_code"]
    ) -> pd.DataFrame:
        path = f"{self.utilities_path}/country_lookup.xlsx"
        data = await self.read_blob(path=path)
        df = pd.read_excel(BytesIO(data), sheet_name=f"{sheet}_lookup")
        return df
    
    @staticmethod
    def with_storage_manager(func):
        async def wrapper(*args, **kwargs):
            async with StorageManager() as storage_manager:
                logger.debug("Connected to Azure blob")
                return await func(*args, storage_manager=storage_manager, **kwargs)
        return wrapper