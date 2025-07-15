import logging
import math
import os
from dataclasses import dataclass, field

from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import ContainerClient

from ._base import BaseStorage

__all__ = [
    "CONTAINER_NAME",
    "FOLDER_NAME",
    "STORAGE_OPTIONS",
    "AzureStorage",
    "StorageManager",
]


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


CONTAINER_NAME = os.environ["AZURE_STORAGE_CONTAINER_NAME"]
FOLDER_NAME = os.environ["AZURE_STORAGE_FOLDER_NAME"]
STORAGE_OPTIONS = {
    "account_name": os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
    "sas_token": os.environ["AZURE_STORAGE_SAS_TOKEN"],
}


@dataclass(frozen=True)
class AzureStorage(BaseStorage):
    """
    Storage interface for Azure Blob Storage.
    """

    container_name: str = CONTAINER_NAME
    storage_options: dict = field(default_factory=lambda: STORAGE_OPTIONS)

    def join_path(self, file_path: str) -> str:
        return f"az://{self.container_name}/{file_path}"


class StorageManager:
    def __init__(self, clear_cache: bool = False):
        self.container_client = ContainerClient.from_container_url(
            container_url=os.environ["AZURE_STORAGE_SAS_URL"]
        )
        self.clear_cache = clear_cache
        self.sources_cfg_path = "config/sources"
        self.utilities_path = "config/utilities"
        self.sources_path = "sources/raw"
        self.output_path = os.environ.get("STORAGE_OUTPUT_PATH", "output")
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

    async def close(self):
        await self.container_client.close()

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

    @staticmethod
    def with_storage_manager(func):
        async def wrapper(*args, **kwargs):
            async with StorageManager() as storage_manager:
                logger.debug("Connected to Azure blob")
                return await func(*args, storage_manager=storage_manager, **kwargs)

        return wrapper
