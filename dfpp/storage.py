import asyncio
import configparser
import logging
from typing import Any, AsyncGenerator, Dict, Generator, List, Optional, Tuple
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient, ContentSettings
from azure.storage.blob.aio import BlobPrefix
from azure.storage.blob.aio import ContainerClient as AContainerClient
import os
from dfpp.dfpp_exceptions import ConfigError, DFPSourceError
import math
import tempfile
import ast
import itertools

logger = logging.getLogger(__name__)
ROOT_FOLDER = os.environ.get('ROOT_FOLDER')
MANDATORY_SOURCE_COLUMNS = 'id', 'url', 'save_as'
TMP_SOURCES = {}


class UnescapedConfigParser(configparser.RawConfigParser):
    """
    An extension of the RawConfigParser that does not escape values when reading from a file.
    """

    def get(self, section, option, **kwargs):
        """
        Get the value of an option.
        :param section: The section of the config file.
        :param option: The option to get the value of.
        :param kwargs: The keyword arguments.
        :return:
        """
        value = super().get(section, option, **kwargs)
        try:
            return value.encode().decode('unicode_escape')
        except AttributeError:
            return value


def chunker(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


def cfg2dict(config_object=None):
    """
    Copnverts a config object to dict
    :param config_object:
    :return: dict
    """
    output_dict = dict()
    sections = config_object.sections()
    for section in sections:
        items = config_object.items(section)
        output_dict[section] = dict(items)
    return output_dict


def validate_src_cfg(cfg_dict=None, cfg_file_path=None):
    """
    Validate a source config file
    :param cfg_dict:
    :param cfg_file_path:
    :return:
    """
    assert cfg_dict is not None, f'Invalid source config {cfg_dict}'
    assert cfg_dict != {}, f'Invalid source config {cfg_dict}'

    for k in MANDATORY_SOURCE_COLUMNS:
        v = cfg_dict[k]
        try:
            v_parsed = ast.literal_eval(v)
            assert v_parsed is not None, f"{k} key {cfg_file_path} needs to be a valid string. Current value is {v}"
        except AssertionError:
            raise
        except Exception as e:
            pass

        assert k in cfg_dict, f'{cfg_file_path} needs to contain {k} key'
        assert isinstance(v, str), f"{cfg_file_path}'s {k} key needs to be a string. Current value is {type(v)}"
        assert v, f"{cfg_file_path}'s {k} key needs to be a valid string. Current value is {v}"

#
# class AzureBlobStorageManager:
#     """
#     A class to manage Azure Blob Storage operations.
#     Implements singleton pattern to ensure only one instance is created.
#     """
#
#     _instance = None
#
#     def __init__(self, connection_string: str = None, container_name: str = None):
#         """
#         Initializes the container client for Azure Blob Storage.
#         """
#         self.delimiter = "/"
#         self.ROOT_FOLDER = ROOT_FOLDER
#         assert self.ROOT_FOLDER not in ('', None), f'Invalid self.ROOT_FOLDER={self.ROOT_FOLDER}'
#
#         if AzureBlobStorageManager._instance is not None:
#             raise Exception(
#                 "Use create_instance() to get a singleton instance of this class"
#             )
#         else:
#             self.container_client = ContainerClient.from_connection_string(
#                 conn_str=connection_string, container_name=container_name
#             )
#
#     @staticmethod
#     def create_instance(
#             connection_string: str = None,
#             container_name: str = None,
#             use_singleton: bool = True,
#     ):
#         """
#         Creates and initializes an instance of the AzureBlobStorageManager class
#         using the Singleton pattern. If an instance already exists and use_singleton is True,
#         it returns the existing instance. If use_singleton is False, it always creates a new instance.
#
#         :param connection_string: The connection string for the Azure Blob Storage account.
#         :type connection_string: str
#         :param container_name: The name of the container to be used in Azure Blob Storage.
#         :type container_name: str
#         :param use_singleton: A boolean to determine if the Singleton pattern should be used.
#                             If True, return the existing instance if available. If False, create a new instance.
#         :type use_singleton: bool, optional, default=True
#         :return: An initialized instance of the AzureBlobStorageManager class.
#         :rtype: AzureBlobStorageManager
#         """
#         if use_singleton and AzureBlobStorageManager._instance is not None:
#             return AzureBlobStorageManager._instance
#         else:
#             AzureBlobStorageManager._instance = AzureBlobStorageManager(
#                 connection_string=connection_string, container_name=container_name
#             )
#             return AzureBlobStorageManager._instance
#
#     def hierarchical_list(self, prefix: str = None):
#         for blob in self.container_client.walk_blobs(
#                 name_starts_with=prefix, delimiter=self.delimiter
#         ):
#             yield blob
#
#     def _yield_blobs(self, prefix: str = None):
#         for blob in self.container_client.list_blobs(name_starts_with=prefix):
#             yield blob
#
#     def list_blobs(self, prefix: str = None):
#         blob_list = []
#         for blob in self.container_client.list_blobs(name_starts_with=prefix):
#             blob_list.append(blob)
#         return blob_list
#
#     def list_and_filter(self, prefix: str = None, filter: str = None):
#         filtered_blobs = []
#         name_starts_with = f"{prefix}/{filter}"
#         for blob in self.container_client.list_blobs(name_starts_with=name_starts_with):
#             filtered_blobs.append(blob)
#         return filtered_blobs
#
#     def list_indicators(self) -> List[str]:
#         """
#         Synchronously lists indicator configuration files from an Azure Blob Container,
#         and returns a list containing all of the indicator_ids present in the directory.
#
#         Returns:
#             List[str]: A list where each value is an indicator_id taken from individual indicator
#                                     configuration files.
#
#         Raises:
#             ConfigError: Raised if an indicator configuration file is invalid or has a missing "indicator" section.
#         """
#         indicator_list = []
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/sources"
#         for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and "indicators" in blob.name
#             ):
#                 stream = self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = stream.readall()
#                 content_str = content.decode("utf-8")
#                 parser = configparser.ConfigParser(interpolation=None)
#                 parser.read_string(content_str)
#                 if "indicator" in parser:
#                     indicator_list.append(parser["indicator"].get("id"))
#
#                 else:
#                     raise ConfigError(f"Invalid indicator config")
#         return indicator_list
#
#     def list_sources(self) -> List[str]:
#         """
#         Synchronously lists source configuration files from an Azure Blob Container,
#         and returns a list containing all of the source_ids present in the directory.
#
#         Returns:
#             List[str]: A list where each value is a source_id taken from individual source
#                                         configuration files.
#
#         Raises:
#             ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
#         """
#         source_list = []
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/sources"
#         for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and not "indicators" in blob.name
#             ):
#                 stream = self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = stream.readall()
#                 content_str = content.decode("utf-8")
#                 parser = configparser.ConfigParser(interpolation=None)
#                 parser.read_string(content_str)
#                 if "source" in parser:
#                     source_list.append(parser["source"].get("id"))
#                 else:
#                     raise ConfigError(f"Invalid source")
#         return source_list
#
#     def get_source_config(self) -> Dict[str, Dict[str, Any]]:
#         """
#         Synchronously lists source configuration files from an Azure Blob Container,
#         and returns a dictionary containing their parsed content.
#
#         Returns:
#             Dict[str, Dict[str, Any]]: A dictionary where keys are source IDs and values are dictionaries
#                                         containing the key-value pairs of the source configuration.
#
#         Raises:
#             ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
#         """
#         cfg = {}
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/sources"
#         for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and not "indicators" in blob.name
#             ):
#                 stream = self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = stream.readall()
#                 content_str = content.decode("utf-8")
#                 parser = configparser.ConfigParser(interpolation=None)
#                 parser.read_string(content_str)
#                 if "source" in parser:
#                     src_id = parser["source"].get("id")
#                     cfg[src_id] = dict(parser["source"].items())
#                     if "downloader_params" in parser:
#                         cfg[src_id]["downloader_params"] = dict(
#                             parser["downloader_params"].items()
#                         )
#                 else:
#                     raise ConfigError(f"Invalid source")
#         return cfg
#
#     def get_source_indicator_config(self) -> Dict[str, Dict[str, Any]]:
#         """
#         Synchronously lists source indicator configuration files from an Azure Blob Container,
#         and updates the provided 'cfg' dictionary with the parsed content.
#
#         Returns:
#             Dict[str, Dict[str, Any]]: The updated 'cfg' dictionary with the source indicator information
#                                         (keys are source IDs, and values are dictionaries containing source
#                                         configuration and a list of associated indicator IDs).
#
#         Raises:
#             ConfigError: Raised if an indicator configuration file is invalid or has a missing "indicator" section.
#         """
#         cfg = self.get_source_config()
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/sources"
#         for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and "indicators" in blob.name
#             ):
#                 stream = self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = stream.readall()
#                 content_str = content.decode("utf-8")
#                 parser = configparser.ConfigParser(interpolation=None)
#                 parser.read_string(content_str)
#                 if "indicator" in parser:
#                     src_id = parser["indicator"].get("source_id")
#                     indicator_id = parser["indicator"].get("id")
#                     # set indicator value to a list if it does not exist yet, and append the value to the existing/new list
#                     cfg[src_id].setdefault("indicators", []).append(indicator_id)
#                 else:
#                     raise ConfigError(f"Invalid indicator config")
#         return cfg
#
#     def get_utility_file(self, utility_file: str) -> Dict[str, Dict[str, Any]]:
#         """
#         Synchronously retrieves a specified utility configuration file from the Azure Blob Container,
#         parses it, and returns its content as a dictionary.
#
#         Args:
#             utility_file (str): The name of the utility configuration file to search for.
#
#         Returns:
#             dict: A dictionary representation of the utility configuration file content,
#                 where keys are section names and values are dictionaries of key-value pairs within the section.
#
#         Raises:
#             ConfigError: Raised if the specified utility configuration file is not found or not valid.
#
#         """
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/utilities"
#         for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and utility_file == os.path.basename(blob.name)
#             ):
#                 stream = self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = stream.readall()
#                 content_str = content.decode("utf-8")
#                 parser = configparser.ConfigParser(interpolation=None)
#                 parser.read_string(content_str)
#                 config_dict = {
#                     section: dict(parser.items(section))
#                     for section in parser.sections()
#                 }
#                 return config_dict
#         else:
#             raise ConfigError(f"Utitlity source not valid")
#
#     def get_source_files(
#             self,
#             source_type: str,
#             source_files: List[str] = None,
#     ) -> Generator[bytes, None, None]:
#         """
#         Synchronously queries an Azure Blob Container for CSV files in a specific directory,
#         and yields them one by one as a generator.
#
#         Args:
#             source_type (str): The subdirectory under 'sources' to search for CSV files.
#                             Values can either be "raw" or "standardized"
#             source_files (list, optional): A list of source files to search for. If empty or None,
#                                         all CSV files in the directory will be returned. Defaults to None.
#
#         Yields:
#             bytes: The content of a CSV file in bytes.
#
#         Raises:
#             DFPSourceError: Raised if no matching CSV files are found in the specified directory.
#
#         Example usage:
#             for dataset in AzureBlobStorageManager.get_source_files(source_type, source_files):
#                 # Open and process each dataset here
#         """
#         if source_type not in ("raw", "standardized"):
#             raise ValueError("source_type must be either 'raw' or 'standardized'")
#
#         if source_files is None:
#             source_files = []
#
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/sources/{source_type}"
#         found_csv = False
#         for blob in self._yield_blobs(prefix=prefix):
#             if len(source_files) > 0:
#                 for source_id in source_files:
#                     if (
#                             not isinstance(blob, BlobPrefix)
#                             and (
#                             blob.name.endswith(".csv")
#                             or blob.name.endswith(".xlsx")
#                             or blob.name.endswith(".xls")
#                     )
#                             and source_id == os.path.basename(blob.name)
#                     ):
#                         stream = self.container_client.download_blob(
#                             blob.name, max_concurrency=8
#                         )
#                         content = stream.readall()
#                         found_csv = True
#                         yield content
#             elif not isinstance(blob, BlobPrefix) and (
#                     blob.name.endswith(".csv")
#                     or blob.name.endswith(".xlsx")
#                     or blob.name.endswith(".xls")
#             ):
#                 stream = self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = stream.readall()
#                 found_csv = True
#                 yield content
#
#         if not found_csv:
#             raise DFPSourceError(
#                 f"An error occurred returning the source CSVs from the raw directory."
#             )
#
#     def get_output_files(
#             self, subfolder: str
#     ) -> Generator[Tuple[str, bytes], None, None]:
#         # ...
#         """
#         Synchronously retrieves the contents of CSV and JSON files within the "raw" folder
#         inside the specified subfolder directory.
#
#         Args:
#             subfolder (str): The subdirectory under 'output' to search for files.
#                                 Values can either be "access_all_data" or "vaccine_equity"
#
#         Yields:
#             Tuple[str, bytes]: A tuple containing the file name and the content of the file in bytes.
#
#         Raises:
#             ValueError: Raised if subfolder is not "access_all_data" or "vaccine_equity".
#         """
#
#         if subfolder not in ("access_all_data", "vaccine_equity"):
#             raise ValueError(
#                 "output_type must be either 'access_all_data' or 'vaccine_equity'"
#             )
#
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/output/{subfolder}/raw"
#         for blob in self._yield_blobs(prefix=prefix):
#             if not isinstance(blob, BlobPrefix):
#                 stream = self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = stream.readall()
#                 yield blob.name, content
#
#     def delete(self, blob_name: str = None) -> None:
#         blob_client = self.container_client.get_blob_client(blob=blob_name)
#         blob_client.delete_blob()
#
#     def download(self, blob_name: str = None, dst_path: str = None) -> Optional[bytes]:
#         """
#         Downloads a file from Azure Blob Storage and returns its data or saves it to a local file.
#
#         Args:
#             blob_name (str, optional): The name of the blob to download. Defaults to None.
#             dst_path (str, optional): The local path to save the downloaded file. If not provided, the file data is returned instead of being saved to a file. Defaults to None.
#
#         Returns:
#             bytes or None: The data of the downloaded file, or None if a dst_path argument is provided.
#         """
#         blob_client = self.container_client.get_blob_client(blob=blob_name)
#         data = blob_client.download_blob()
#
#         if dst_path:
#             with open(dst_path, "wb") as f:
#                 data.readinto(f)
#             return None
#         else:
#             return data.content_as_bytes()
#
#     def check_blob_exists(self, blob_name: str) -> bool:
#         """
#         Checks whether a blob exists in Azure Blob Storage.
#
#         Args:
#             blob_name (str): The name of the blob to check.
#
#         Returns:
#             bool: True if the blob exists, False otherwise.
#         """
#         blob_client = self.container_client.get_blob_client(blob=blob_name)
#         return blob_client.exists()
#
#     def copy_blob(self, src_blob: str, dst_blob: str) -> None:
#         """
#         Copies a blob from one location to another in Azure Blob Storage.
#
#         Args:
#             src_blob (str): The name of the source blob.
#             dst_blob (str): The name of the destination blob.
#         """
#         blob_client = self.container_client.get_blob_client(blob=src_blob)
#         blob_client.start_copy_from_url(
#             self.container_client.primary_hostname + "/" + self.container_name + "/" + dst_blob
#         )
#
#     def upload(
#             self,
#             dst_path: str = None,
#             src_path: str = None,
#             data: bytes = None,
#             content_type: str = None,
#             overwrite: bool = True,
#     ) -> None:
#         """
#             Uploads a file or bytes data to Azure Blob Storage.
#
#         def upload(self, dst_path: str = None, src_path: str = None, overwrite=None, content_type=None) -> None:
#             Args:
#                 dst_path (str, optional): The path of the destination blob in Azure Blob Storage. Defaults to None.
#                 src_path (str, optional): The local path of the file to upload. Either src_path or data must be provided. Defaults to None.
#                 data (bytes, optional): The bytes data to upload. Either src_path or data must be provided. Defaults to None.
#                 content_type (str, optional): The content type of the blob. Defaults to None.
#                 overwrite (bool, optional): Whether to overwrite the destination blob if it already exists. Defaults to True.
#
#             Raises:
#                 ValueError: If neither src_path nor data are provided.
#
#             Returns:
#                 None
#         """
#
#         def _progress_(current, total) -> None:
#             progress = current / total * 100
#             logger.info(f'uploaded - {progress}%')
#
#         blob_client = self.container_client.get_blob_client(blob=dst_path)
#
#         if src_path:
#             with open(src_path, "rb") as f:
#                 blob_client.upload_blob(
#                     data=f,
#                     overwrite=overwrite,
#                     content_settings=ContentSettings(content_type=content_type),
#                     max_concurrency=8,
#                     progress_hook=_progress_
#                 )
#         elif data:
#             blob_client.upload_blob(
#                 data=data,
#                 overwrite=overwrite,
#                 content_settings=ContentSettings(content_type=content_type),
#                 max_concurrency=8,
#                 progress_hook=_progress_
#             )
#         else:
#             raise ValueError("Either 'src_path' or 'data' must be provided.")
#
#
# class AsyncAzureBlobStorageManager:
#     """
#     An asynchronous class to manage Azure Blob Storage operations.
#     Implements singleton pattern to ensure only one instance is created.
#     """
#
#     _instance = None
#
#     def __init__(self, container_client):
#         """
#         Initializes a new instance of the AsyncAzureBlobStorageManager class.
#
#         Note: This method should not be called directly. Use the create_instance() method instead.
#
#         :param container_client: An instance of the ContainerClient class.
#         :type container_client: ContainerClient
#         """
#         self.container_client = container_client
#         self.delimiter = "/"
#         self.ROOT_FOLDER = ROOT_FOLDER
#         assert self.ROOT_FOLDER not in ('', None), f'Invalid self.ROOT_FOLDER={self.ROOT_FOLDER}'
#
#     @classmethod
#     async def create_instance(
#             cls: "AsyncAzureBlobStorageManager",
#             connection_string: str = None,
#             container_name: str = None,
#             use_singleton: bool = True,
#     ):
#         """
#         Asynchronously creates and initializes an instance of the AsyncAzureBlobStorageManager class
#         using the Singleton pattern. If an instance already exists and use_singleton is True,
#         it returns the existing instance. If use_singleton is False, it always creates a new instance.
#
#         :param cls: A singleton class instance of AsyncAzureBlobStorageManager.
#         :type: Type[AsyncAzureBlobStorageManager]
#         :param connection_string: The connection string for the Azure Blob Storage account.
#         :type connection_string: str
#         :param container_name: The name of the container to be used in Azure Blob Storage.
#         :type container_name: str
#         :param use_singleton: A boolean to determine if the Singleton pattern should be used.
#                             If True, return the existing instance if available. If False, create a new instance.
#         :type use_singleton: bool, optional, default=True
#         :return: An initialized instance of the AsyncAzureBlobStorageManager class.
#         :rtype: AsyncAzureBlobStorageManager
#         """
#         try:
#             if use_singleton:
#                 if cls._instance is None:
#                     # Create an instance if it doesn't exist
#                     container_client = AContainerClient.from_connection_string(
#                         conn_str=connection_string, container_name=container_name
#                     )
#                     cls._instance = cls(container_client)
#                 return cls._instance
#             else:
#                 # Always create a new instance
#                 container_client = AContainerClient.from_connection_string(
#                     conn_str=connection_string, container_name=container_name
#                 )
#                 return cls(container_client)
#         except Exception as e:
#             raise e
#
#     @staticmethod
#     async def _initialize(container_client):
#         """
#         Asynchronously checks if the Azure Blob Storage container exists.
#
#         :param container_client: An instance of the ContainerClient class.
#         :type container_client: ContainerClient
#         :raises ResourceNotFoundError: If the container does not exist.
#         """
#         try:
#             await container_client.get_container_properties()
#         except ResourceNotFoundError as e:
#             raise ResourceNotFoundError(
#                 f"The container '{container_client.container_name}' does not exist."
#             ) from e
#
#     async def hierarchical_list(self, prefix: str = None):
#         async for blob in self.container_client.walk_blobs(
#                 name_starts_with=prefix, delimiter=self.delimiter
#         ):
#             yield blob
#
#     async def list_blobs(self, prefix: str = None):
#         blob_list = []
#         async for blob in self.container_client.list_blobs(name_starts_with=prefix):
#             blob_list.append(blob)
#         return blob_list
#
#     async def _yield_blobs(self, prefix: str = None):
#         async for blob in self.container_client.list_blobs(name_starts_with=prefix):
#             yield blob
#
#     async def list_and_filter(self, prefix: str = None, filter: str = None):
#         filtered_blobs = []
#         name_starts_with = f"{prefix}/{filter}"
#         async for blob in self.container_client.list_blobs(
#                 name_starts_with=name_starts_with
#         ):
#             filtered_blobs.append(blob)
#         return filtered_blobs
#
#     async def list_indicators(self) -> List[str]:
#         """
#         Asynchronously lists indicator configuration files from an Azure Blob Container,
#         and returns a list containing all of the indicator_ids present in the directory.
#
#         Returns:
#             List[str]: A list where each value is an indicator_id taken from individual indicator
#                                     configuration files.
#
#         Raises:
#             ConfigError: Raised if an indicator configuration file is invalid or has a missing "indicator" section.
#         """
#         indicator_list = []
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/sources"
#         async for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and "indicators" in blob.name
#             ):
#                 stream = await self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = await stream.readall()
#                 content_str = content.decode("utf-8")
#                 parser = configparser.ConfigParser(interpolation=None)
#                 parser.read_string(content_str)
#                 if "indicator" in parser:
#                     indicator_list.append(parser["indicator"].get("id"))
#
#                 else:
#                     raise ConfigError(f"Invalid indicator config")
#         return indicator_list
#
#     async def list_sources(self) -> List[str]:
#         """
#         Asynchronously lists source configuration files from an Azure Blob Container,
#         and returns a list containing all of the source_ids present in the directory.
#
#         Returns:
#             List[str]: A list where each value is a source_id taken from individual source
#                                         configuration files.
#
#         Raises:
#             ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
#         """
#         source_list = []
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/sources"
#         async for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and not "indicators" in blob.name
#             ):
#                 stream = await self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = await stream.readall()
#                 content_str = content.decode("utf-8")
#                 parser = configparser.ConfigParser(interpolation=None)
#                 parser.read_string(content_str)
#                 if "source" in parser:
#                     source_list.append(parser["source"].get("id"))
#                 else:
#                     raise ConfigError(f"Invalid source")
#         return source_list
#
#     async def dsource(self, blob_name):
#         logger.debug(f'Fetching source config for {blob_name}')
#         stream = await self.container_client.download_blob(
#             blob_name, max_concurrency=1
#         )
#         content = await stream.readall()
#         content_str = content.decode("utf-8")
#         parser = configparser.ConfigParser(interpolation=None)
#         parser.read_string(content_str)
#         if "source" in parser:
#             src_id = parser["source"].get("id")
#             cfg = dict(parser["source"].items())
#             if "downloader_params" in parser:
#                 downloader_params = dict(
#                     parser["downloader_params"].items()
#                 )
#             else:
#                 downloader_params = None
#             return src_id, cfg, downloader_params
#
#     async def get_source_config_conc(self) -> Dict[str, Dict[str, Any]]:
#         """
#         Asynchronously lists source configuration files from an Azure Blob Container,
#         and returns a dictionary containing their parsed content.
#
#         Returns:
#             Dict[str, Dict[str, Any]]: A dictionary where keys are source IDs and values are dictionaries
#                                         containing the key-value pairs of the source configuration.
#
#         Raises:
#             ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
#         """
#         cfg = {}
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/sources"
#         futures = []
#         async for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and not "indicators" in blob.name
#             ):
#                 futures.append(asyncio.ensure_future(
#                     self.dsource(blob_name=blob.name)
#                 ))
#
#         for fut in asyncio.as_completed(futures):
#             src_id, src_cfg, downloader_params = await fut
#             cfg[src_id] = src_cfg
#             cfg[src_id]['downloader_params'] = downloader_params
#
#         return cfg
#
#     async def get_source_config(self) -> Dict[str, Dict[str, Any]]:
#         """
#         Asynchronously lists source configuration files from an Azure Blob Container,
#         and returns a dictionary containing their parsed content.
#
#         Returns:
#             Dict[str, Dict[str, Any]]: A dictionary where keys are source IDs and values are dictionaries
#                                         containing the key-value pairs of the source configuration.
#
#         Raises:
#             ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
#         """
#         cfg = {}
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/sources"
#         async for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and not "indicators" in blob.name
#             ):
#                 stream = await self.container_client.download_blob(
#                     blob.name, max_concurrency=1
#                 )
#                 content = await stream.readall()
#                 content_str = content.decode("utf-8")
#                 parser = configparser.ConfigParser(interpolation=None)
#                 parser.read_string(content_str)
#                 if "source" in parser:
#                     src_id = parser["source"].get("id")
#                     cfg[src_id] = dict(parser["source"].items())
#                     if "downloader_params" in parser:
#                         cfg[src_id]["downloader_params"] = dict(
#                             parser["downloader_params"].items()
#                         )
#                 else:
#                     raise ConfigError(f"Invalid source")
#         return cfg
#
#     async def get_source_indicator_config(self) -> Dict[str, Dict[str, Any]]:
#         """
#         Asynchronously lists source indicator configuration files from an Azure Blob Container,
#         and updates the provided 'cfg' dictionary with the parsed content.
#
#         Returns:
#             Dict[str, Dict[str, Any]]: The updated 'cfg' dictionary with the source indicator information
#                                         (keys are source IDs, and values are dictionaries containing source
#                                         configuration and a list of associated indicator IDs).
#
#         Raises:
#             ConfigError: Raised if an indicator configuration file is invalid or has a missing "indicator" section.
#         """
#         cfg = await self.get_source_config()
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/config/sources"
#         async for blob in self._yield_blobs(prefix=prefix):
#             if (
#                     not isinstance(blob, BlobPrefix)
#                     and blob.name.endswith(".cfg")
#                     and "indicators" in blob.name
#             ):
#                 stream = await self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = await stream.readall()
#                 content_str = content.decode("utf-8")
#                 parser = configparser.ConfigParser(interpolation=None)
#                 parser.read_string(content_str)
#                 if "indicator" in parser:
#                     src_id = parser["indicator"].get("source_id")
#                     indicator_id = parser["indicator"].get("id")
#                     # set indicator value to a list if it does not exist yet, and append the value to the existing/new list
#                     cfg[src_id].setdefault("indicators", []).append(indicator_id)
#                 else:
#                     raise ConfigError(f"Invalid indicator config")
#         return cfg
#
#     async def get_source_files(
#             self,
#             source_type: str,
#             source_files: List[str] = None,
#     ) -> AsyncGenerator[bytes, None]:
#         """
#         Asynchronously queries an Azure Blob Container for CSV files in a specific directory,
#         and yields them one by one as a generator.
#
#         Args:
#             source_type (str): The subdirectory under 'sources' to search for CSV files.
#                             Values can either be "raw" or "standardized"
#             source_files (list, optional): A list of source files to search for. If empty or None,
#                                         all CSV files in the directory will be returned. Defaults to None.
#
#         Yields:
#             bytes: The content of a CSV file in bytes.
#
#         Raises:
#             DFPSourceError: Raised if no matching CSV files are found in the specified directory.
#
#         Example usage:
#             async for dataset in AsyncAzureBlobStorageManager.get_source_files(source_type, source_files):
#                 # Open and process each dataset here
#         """
#         if source_type not in ("raw", "standardized"):
#             raise ValueError("source_type must be either 'raw' or 'standardized'")
#
#         if source_files is None:
#             source_files = []
#
#         found_csv = False
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/sources/{source_type}"
#         async for blob in self._yield_blobs(prefix=prefix):
#             if len(source_files) > 0:
#                 for source_id in source_files:
#                     if (
#                             not isinstance(blob, BlobPrefix)
#                             and (
#                             blob.name.endswith(".csv")
#                             or blob.name.endswith(".xlsx")
#                             or blob.name.endswith(".xls")
#                     )
#                             and source_id == os.path.basename(blob.name)
#                     ):
#                         stream = await self.container_client.download_blob(
#                             blob.name, max_concurrency=8
#                         )
#                         content = await stream.readall()
#                         found_csv = True
#                         yield content
#             elif not isinstance(blob, BlobPrefix) and (
#                     blob.name.endswith(".csv")
#                     or blob.name.endswith(".xlsx")
#                     or blob.name.endswith(".xls")
#             ):
#                 stream = await self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = await stream.readall()
#                 found_csv = True
#                 yield content
#
#         if not found_csv:
#             raise DFPSourceError(
#                 f"An error occurred returning the source CSVs from the raw directory."
#             )
#
#     async def get_output_files(
#             self, subfolder: str
#     ) -> AsyncGenerator[Tuple[str, bytes], None]:
#         # ...
#         """
#         Asynchronously retrieves the contents of CSV and JSON files within the "raw" folder
#         inside the specified subfolder directory.
#
#         Args:
#             subfolder (str): The subdirectory under 'output' to search for files.
#                                 Values can either be "access_all_data" or "vaccine_equity"
#
#         Yields:
#             Tuple[str, bytes]: A tuple containing the file name and the content of the file in bytes.
#
#         Raises:
#             ValueError: Raised if subfolder is not "access_all_data" or "vaccine_equity".
#         """
#
#         if subfolder not in ("access_all_data", "vaccine_equity"):
#             raise ValueError(
#                 "output_type must be either 'access_all_data' or 'vaccine_equity'"
#             )
#
#         # os.path.join doesn't work for filtering returned Azure blob paths
#         prefix = f"{self.ROOT_FOLDER}/output/{subfolder}/raw"
#         async for blob in self._yield_blobs(prefix=prefix):
#             if not isinstance(blob, BlobPrefix):
#                 stream = await self.container_client.download_blob(
#                     blob.name, max_concurrency=8
#                 )
#                 content = await stream.readall()
#                 yield blob.name, content
#
#     async def delete(self, blob_name: str = None) -> None:
#         blob_client = self.container_client.get_blob_client(blob=blob_name)
#         await blob_client.delete_blob()
#
#     async def download(
#             self, blob_name: str = None, dst_path: str = None
#     ) -> Optional[bytes]:
#         """
#         Downloads a file from Azure Blob Storage and returns its data or saves it to a local file.
#
#         Args:
#             blob_name (str, optional): The name of the blob to download. Defaults to None.
#             dst_path (str, optional): The local path to save the downloaded file. If not provided, the file data is returned instead of being saved to a file. Defaults to None.
#
#         Returns:
#             bytes or None: The data of the downloaded file, or None if a dst_path argument is provided.
#         """
#         # _, b_name = os.path.split(blob_name)
#         #
#         # async def _progress_(current, total) -> None:
#         #     progress = current / total * 100
#         #     rounded_progress = int(math.floor(progress))
#         #     logger.info(f'{b_name} was downloaded - {rounded_progress}%')
#         logger.debug(f'Downloading {blob_name}')
#         blob_client = self.container_client.get_blob_client(blob=blob_name)
#         chunk_list = []
#         stream = await blob_client.download_blob()
#         async for chunk in stream.chunks():
#             chunk_list.append(chunk)
#
#         # data = await blob_client.download_blob().chunks()
#         data = b"".join(chunk_list)
#         logger.debug(f'Finished downloading {blob_name}')
#         if dst_path:
#             async with open(dst_path, "wb") as f:
#                 f.write(data)
#             return None
#         else:
#             return data
#
#     async def upload(
#             self,
#             dst_path: str = None,
#             src_path: str = None,
#             data: bytes = None,
#             content_type: str = None,
#             overwrite: bool = True,
#     ) -> None:
#         """
#             Uploads a file or bytes data to Azure Blob Storage.
#
#         async def upload(self, dst_path: str = None, src_path: str = None, content_type=None,
#                          overwrite: bool = True) -> None:
#             Args:
#                 dst_path (str, optional): The path of the destination blob in Azure Blob Storage. Defaults to None.
#                 src_path (str, optional): The local path of the file to upload. Either src_path or data must be provided. Defaults to None.
#                 data (bytes, optional): The bytes data to upload. Either src_path or data must be provided. Defaults to None.
#                 content_type (str, optional): The content type of the blob. Defaults to None.
#                 overwrite (bool, optional): Whether to overwrite the destination blob if it already exists. Defaults to True.
#
#             Raises:
#                 ValueError: If neither src_path nor data are provided.
#
#             Returns:
#                 None
#         """
#
#         try:
#             _, blob_name = os.path.split(dst_path)
#
#             async def _progress_(current, total) -> None:
#                 progress = current / total * 100
#                 rounded_progress = int(math.floor(progress))
#                 logger.info(f'{blob_name} was uploaded - {rounded_progress}%')
#
#             blob_client = self.container_client.get_blob_client(blob=dst_path)
#             if src_path:
#                 with open(src_path, "rb") as f:
#                     await blob_client.upload_blob(
#                         data=f,
#                         overwrite=overwrite,
#                         content_settings=ContentSettings(content_type=content_type),
#                         progress_hook=_progress_
#                     )
#             elif data:
#                 await blob_client.upload_blob(
#                     data=data,
#                     overwrite=overwrite,
#                     content_settings=ContentSettings(content_type=content_type),
#                     progress_hook=_progress_,
#
#                 )
#             else:
#                 raise ValueError("Either 'src_path' or 'data' must be provided.")
#         except Exception as e:
#             raise e
#
#     async def check_blob_exists(self, blob_name: str) -> bool:
#         """
#         Checks if a blob exists in the container.
#         Args:
#             blob_name (str): The name of the blob to check.
#         Returns:
#             bool: True if the blob exists, False otherwise.
#         """
#         blob_client = self.container_client.get_blob_client(blob=blob_name)
#         return await blob_client.exists()
#
#     async def copy_blob(self, src_blob: str, dst_blob: str) -> None:
#         """
#         Copies a blob from one location to another within the same container.
#
#         Args:
#             src_blob (str): The name of the source blob.
#             dst_blob (str): The name of the destination blob.
#
#         Returns:
#             None
#         """
#         src_blob_client = self.container_client.get_blob_client(blob=src_blob)
#         dst_blob_client = self.container_client.get_blob_client(blob=dst_blob)
#         await dst_blob_client.start_copy_from_url(src_blob_client.url)
#
#     async def close(self) -> None:
#         """
#         Closes the connection to the Azure Blob Storage container.
#         :return:
#         """
#         await self.container_client.close()
#

class StorageManager:
    REL_INDICATORS_CFG_PATH = 'config/indicators'
    REL_SOURCES_CFG_PATH = 'config/sources'
    REL_UTILITIES_PATH = 'config/utilities'
    REL_SOURCES_PATH = 'sources/raw'
    REL_OUTPUT_PATH = 'output'

    def __init__(self, connection_string: str = None, container_name: str = None, root_folder=ROOT_FOLDER,
                 use_singleton: bool = False):
        self.container_client = AContainerClient.from_connection_string(conn_str=connection_string,
                                                                        container_name=container_name)
        self.container_name = container_name
        self.root_folder = root_folder
        self.INDICATORS_CFG_PATH = os.path.join(self.root_folder, self.REL_INDICATORS_CFG_PATH)
        self.SOURCES_CFG_PATH = os.path.join(self.root_folder, self.REL_SOURCES_CFG_PATH)
        self.UTILITIES_PATH = os.path.join(self.root_folder, self.REL_UTILITIES_PATH)
        self.SOURCES_PATH = os.path.join(self.root_folder, self.REL_SOURCES_PATH)
        self.OUTPUT_PATH = os.path.join(self.root_folder, self.REL_OUTPUT_PATH)

        self.use_singleton = use_singleton

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __str__(self):
        return (f'{self.__class__.__name__}\n'

                f'\t connected to container "{self.container_name}"\n'
                f'\t ROOT_FOLDER: {self.root_folder}\n'
                f'\t INDICATORS_CFG_PATH: {self.INDICATORS_CFG_PATH}\n'
                f'\t SOURCES_CFG_PATH: {self.SOURCES_CFG_PATH}\n'
                f'\t UTILITIES_PATH: {self.UTILITIES_PATH}\n'
                f'\t SOURCES_PATH: {self.SOURCES_PATH}\n'
                f'\t OUTPUT_PATH: {self.OUTPUT_PATH}\n')

    async def list_indicators(self):
        logger.info(f'Listing {self.INDICATORS_CFG_PATH}')
        async for blob in self.container_client.list_blobs(prefix=self.INDICATORS_CFG_PATH):
            if (
                    not isinstance(blob, BlobPrefix)
                    and blob.name.endswith(".cfg")
                    and "indicators" in blob.name
            ): yield blob

    async def list_sources_cfgs(self):
        """
        List all the source cfgs in the container
        :return:
        """
        logger.info(f'Listing {self.SOURCES_CFG_PATH}')
        cfgs = []
        async for blob in self.container_client.list_blobs(prefix=self.SOURCES_CFG_PATH):
            if not isinstance(blob, BlobPrefix) and blob.name.endswith(".cfg") and "sources" in blob.name:
                cfgs.append(blob.name)
        return cfgs

    async def get_indicator_cfg(self, indicator_id: str = None, indicator_path=None):

        try:
            if indicator_id:
                assert indicator_path is None, f'use either indicator_id or indicator_path'
                indicator_path = f'{os.path.join(self.INDICATORS_CFG_PATH, indicator_id)}.cfg'
            else:
                _, indicator_name = os.path.split(indicator_path)
                indicator_id, ext = os.path.splitext(indicator_name)

            # if 'wbentp1_wb' in indicator_path:raise Exception('forced')
            # if indicator_id == "mmrlatest_gii":
            #     print(indicator_id, indicator_path)
            #     exit()
            assert await self.check_blob_exists(
                indicator_path), f'Indicator {indicator_id} located at {indicator_path} does not exist'

            # TODO caching

            logger.info(f'Fetching indicator cfg for {indicator_id} from  {indicator_path}')
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
                    f"Indicator  {indicator_id} located at {indicator_path} does not contain an 'indicator' section")
        except Exception as e:
            logger.error(f'Indicator {indicator_id} will be skipped because {e}')

    async def get_indicators_cfgs(self, contain_filter: str = None, indicator_ids: List = None):
        tasks = []
        if indicator_ids:
            for indicator_id in indicator_ids:
                t = asyncio.create_task(
                    self.get_indicator_cfg(indicator_id=indicator_id)
                )
                tasks.append(t)
            results = await asyncio.gather(*tasks)
        elif contain_filter:
            async for indicator_blob in self.list_indicators():
                if contain_filter and contain_filter not in indicator_blob.name: continue

                t = asyncio.create_task(
                    self.get_indicator_cfg(indicator_path=indicator_blob.name)
                )
                tasks.append(t)
            results = await asyncio.gather(*tasks)
        else:
            async for indicator_blob in self.list_indicators():
                t = asyncio.create_task(
                    self.get_indicator_cfg(indicator_path=indicator_blob.name)
                )
                tasks.append(t)
            results = await asyncio.gather(*tasks)
        return [e for e in results if e]

    async def get_source_cfg(self, source_id=None, source_path=None):
        try:
            if source_id:
                assert source_path is None, f'use either source_id or source_path'
                source_path = f'{os.path.join(self.SOURCES_CFG_PATH, source_id.lower(), f"{source_id.lower()}.cfg")}'
                _, source_name = os.path.split(source_path)
            else:
                _, source_name = os.path.split(source_path)
                source_id, ext = os.path.splitext(source_name)

            # if 'wbentp1_wb' in indicator_path:raise Exception('forced')
            assert await self.check_blob_exists(
                source_path), f'Source {source_id} located at {source_path} does not exist'

            logger.info(f'Fetching source cfg  for {source_id} from  {source_path}')
            # stream = await self.container_client.download_blob(
            #     source_path, max_concurrency=8
            # )
            # content = await stream.readall()
            content = await self.cached_download(source_path=source_path)
            content_str = content.decode("utf-8")

            parser = configparser.ConfigParser(interpolation=None)
            parser.read_string(content_str)
            cfg_dict = cfg2dict(parser)
            validate_src_cfg(cfg_dict=cfg_dict['source'])
            return cfg_dict
        except Exception as e:
            logger.error(f'Source {source_id} will be skipped because {e}')


    async def get_sources_cfgs(self, source_ids: List[str] = None):
        """
        Download and parse source config file of indicators
        :param source_ids:
        :return:
        """
        tasks = []
        if source_ids is None:
            for source_cfg_path in await self.list_sources_cfgs():
                source_id = os.path.split(source_cfg_path)[1].split('.')[0]
                t = asyncio.create_task(
                    self.get_source_cfg(source_id=source_id)
                )
                tasks.append(t)
        results = await asyncio.gather(*tasks)
        return [e for e in results if e]

    async def close(self):
        await self.container_client.close()

    async def get_utility_file(self, utility_file: str) -> Dict[str, Dict[str, Any]]:
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
        prefix = f"{self.UTILITIES_PATH}"
        async for blob in self.container_client.list_blobs(prefix=prefix):
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
            raise ConfigError(f"Utitlity source not valid")

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
                progress = current / total * 100
                rounded_progress = int(math.floor(progress))
                logger.info(f'{blob_name} was uploaded - {rounded_progress}%')

            blob_client = self.container_client.get_blob_client(blob=dst_path)
            if src_path:
                with open(src_path, "rb") as f:
                    await blob_client.upload_blob(
                        data=f,
                        overwrite=overwrite,
                        content_settings=ContentSettings(content_type=content_type),
                        progress_hook=_progress_
                    )
            elif data:
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

    async def download(
            self, blob_name: str = None, dst_path: str = None
    ) -> Optional[bytes]:
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
        logger.debug(f'Downloading {blob_name}')
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        chunk_list = []
        stream = await blob_client.download_blob()
        async for chunk in stream.chunks():
            chunk_list.append(chunk)

        data = b"".join(chunk_list)
        logger.debug(f'Finished downloading {blob_name}')
        if dst_path:
            async with open(dst_path, "wb") as f:
                f.write(data)
            return None
        else:
            return data

    async def delete_blob(self, blob_path):
        """
        :param blob_path: 
        :return: 
        """
        return self.container_client.delete_blob(blob_path)

    async def list_base_files(self):
        return [blob.name async for blob in self.container_client.list_blobs(
            name_starts_with=os.path.join(self.OUTPUT_PATH, 'access_all_data', 'base/'))]

    async def cached_download(self, source_path=None, chunked=False):
        if source_path in TMP_SOURCES:
            cached_src_path = TMP_SOURCES[source_path]
            logger.info(f'Rereading {source_path} from {cached_src_path} ')
            data = open(cached_src_path, 'rb').read()
        else:

            # check the blob exists in azure storage
            source_path_exists = await self.check_blob_exists(blob_name=source_path)
            if not source_path_exists:
                raise Exception(f'Source {source_path} does not exist')
            stream = await self.container_client.download_blob(
                source_path, max_concurrency=8
            )
            if chunked:
                chunk_list = []
                async for chunk in stream.chunks():
                    chunk_list.append(chunk)
                data = b"".join(chunk_list)
            else:
                data = await stream.readall()

            cached = tempfile.NamedTemporaryFile(mode='wb', delete=False)
            logger.debug(f'Going to cache {source_path} to {cached.name}')
            cached.write(data)
            TMP_SOURCES[source_path] = cached.name
        return data
