import ast
import asyncio
import configparser
import hashlib
import itertools
import logging
import math
import os
import tempfile
from typing import Any, Dict, List, Optional

from azure.storage.blob import ContainerClient, ContentSettings
from azure.storage.blob.aio import BlobPrefix
from azure.storage.blob.aio import ContainerClient as AContainerClient

from dfpp.dfpp_exceptions import ConfigError

logger = logging.getLogger(__name__)
ROOT_FOLDER = os.environ.get('ROOT_FOLDER')
AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
AZURE_STORAGE_CONTAINER_NAME = os.environ.get('AZURE_STORAGE_CONTAINER_NAME')
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
    output_dict = {}
    sections = config_object.sections()
    for section in sections:
        items = config_object.items(section)
        output_dict[section] = dict(items)
    return output_dict


async def dict2cfg(cfg_dict=None):
    """
    Converts a dict to a config object
    :param cfg_dict:
    :return:
    """
    parser = configparser.ConfigParser(interpolation=None)
    for section, options in cfg_dict.items():
        parser[section] = options
    return parser


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


class StorageManager:
    REL_INDICATORS_CFG_PATH = 'config/indicators'
    REL_SOURCES_CFG_PATH = 'config/sources'
    REL_UTILITIES_PATH = 'config/utilities'
    REL_SOURCES_PATH = 'sources/raw'
    REL_OUTPUT_PATH = 'output'

    def __init__(self,
                 connection_string: str = AZURE_STORAGE_CONNECTION_STRING,
                 container_name: str = AZURE_STORAGE_CONTAINER_NAME,
                 root_folder=ROOT_FOLDER,
                 clear_cache: bool = False):
        self.conn_str = connection_string
        self.container_name = container_name
        self.container_client = AContainerClient.from_connection_string(conn_str=self.conn_str,
                                                                        container_name=self.container_name)
        self.sync_container_client = ContainerClient.from_connection_string(conn_str=self.conn_str,
                                                                            container_name=self.container_name)
        self.clear_cache = clear_cache
        self.ROOT_FOLDER = root_folder
        self.INDICATORS_CFG_PATH = os.path.join(self.ROOT_FOLDER, self.REL_INDICATORS_CFG_PATH)
        self.SOURCES_CFG_PATH = os.path.join(self.ROOT_FOLDER, self.REL_SOURCES_CFG_PATH)
        self.UTILITIES_PATH = os.path.join(self.ROOT_FOLDER, self.REL_UTILITIES_PATH)
        self.SOURCES_PATH = os.path.join(self.ROOT_FOLDER, self.REL_SOURCES_PATH)
        self.OUTPUT_PATH = os.path.join(self.ROOT_FOLDER, self.REL_OUTPUT_PATH)
        self.BACKUP_PATH = os.path.join(self.ROOT_FOLDER, 'backup')

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __str__(self):
        return (f'{self.__class__.__name__}\n'

                f'\t connected to container "{self.container_name}"\n'
                f'\t ROOT_FOLDER: {self.ROOT_FOLDER}\n'
                f'\t INDICATORS_CFG_PATH: {self.INDICATORS_CFG_PATH}\n'
                f'\t SOURCES_CFG_PATH: {self.SOURCES_CFG_PATH}\n'
                f'\t UTILITIES_PATH: {self.UTILITIES_PATH}\n'
                f'\t SOURCES_PATH: {self.SOURCES_PATH}\n'
                f'\t OUTPUT_PATH: {self.OUTPUT_PATH}\n')

    async def get_md5_checksum(self, blob_name: str = None, data: bytes = None):
        """
        :param blob_name:
        :param data:
        :return:
        """
        assert blob_name is not None, f'blob_name is None'
        # assert await self.check_blob_exists(blob_name), f'Blob {blob_name} does not exist'
        if await self.check_blob_exists(blob_name):
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            properties = await blob_client.get_blob_properties()
            return properties['content_settings']['content_md5']
        else:
            if data is not None:
                md5_hash = hashlib.md5(data).hexdigest()
                return md5_hash
            else:
                raise ValueError("Data is None and the blob does not exist.")

    async def list_indicators(self):
        logger.info(f'Listing {self.INDICATORS_CFG_PATH}')
        async for blob in self.container_client.list_blobs(name_starts_with=self.INDICATORS_CFG_PATH):
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
        async for blob in self.container_client.list_blobs(name_starts_with=self.SOURCES_CFG_PATH):
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
            raise

    async def get_indicators_cfg(self, contain_filter: str = None, indicator_ids: List = None):

        tasks = []

        if contain_filter:
            async for indicator_blob in self.list_indicators():

                if contain_filter and contain_filter not in indicator_blob.name: continue

                t = asyncio.create_task(
                    self.get_indicator_cfg(indicator_path=indicator_blob.name)
                )
                tasks.append(t)
        else:
            if indicator_ids is None:
                async for indicator_blob in self.list_indicators():
                    t = asyncio.create_task(
                        self.get_indicator_cfg(indicator_path=indicator_blob.name)
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
                assert source_path is None, f'use either source_id or source_path'
                source_path = f'{os.path.join(self.SOURCES_CFG_PATH, source_id.lower(), f"{source_id.lower()}.cfg")}'
                _, source_name = os.path.split(source_path)
            else:
                _, source_name = os.path.split(source_path)
                source_id, ext = os.path.splitext(source_name)

            # if 'wbentp1_wb' in indicator_path:raise Exception('forced')
            logger.debug(f'Checking if {source_path} exists')
            assert await self.check_blob_exists(
                source_path), f'Source {source_id} located at {source_path} does not exist'

            logger.info(f'Fetching source cfg  for {source_id} from  {source_path}')
            # stream = await self.container_client.download_blob(
            #     source_path, max_concurrency=8
            # )
            # content = await stream.readall()
            content = await self.cached_download(source_path=source_path)
            logger.debug(f'Downloaded {source_path}')
            content_str = content.decode("utf-8")

            parser = UnescapedConfigParser()
            parser.read_string(content_str)
            logger.debug("Source config read by parser")
            cfg_dict = cfg2dict(parser)
            validate_src_cfg(cfg_dict=cfg_dict['source'])
            return cfg_dict
        except Exception as e:
            logger.error(f'Failed to download {source_id}')
            logger.error(e)
            raise

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
        else:
            if source_ids:
                for source_id in source_ids:
                    t = asyncio.create_task(
                        self.get_source_cfg(source_id=source_id)
                    )
                    tasks.append(t)
        results = await asyncio.gather(*tasks)

        return [e for e in results if e]

    async def close(self):
        await self.container_client.close()
        if self.clear_cache:
            for k, v in TMP_SOURCES.items():
                exists = os.path.exists(v)
                if exists:
                    logger.info(f'Removing cache {v} for source {k} ')
                    os.remove(v)

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
        async for blob in self.container_client.list_blobs(name_starts_with=prefix):
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
                logger.info(f'{blob_name} was uploaded - {rounded_progress}%')

            blob_client = self.container_client.get_blob_client(blob=dst_path)
            if src_path:
                with open(src_path, "rb") as f:
                    logger.debug(f'Uploading {src_path} to {dst_path}')
                    await blob_client.upload_blob(
                        data=f,
                        overwrite=overwrite,
                        content_settings=ContentSettings(content_type=content_type),
                        progress_hook=_progress_
                    )
            elif data:
                logger.debug(f'Uploading bytes/data to {dst_path}')
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

    def sync_upload(self,
                    dst_path: str = None,
                    src_path: str = None,
                    data: bytes = None,
                    content_type: str = None,
                    overwrite: bool = True,
                    max_concurrency: int = 8
                    ):

        try:

            _, blob_name = os.path.split(dst_path)

            def _progress_(current, total) -> None:
                progress = current / total * 100
                rounded_progress = int(math.floor(progress))
                logger.info(f'{blob_name} was uploaded - {rounded_progress}%')

            blob_client = self.sync_container_client.get_blob_client(blob=dst_path)
            if src_path:
                with open(src_path, "rb") as f:
                    logger.debug(f'Uploading {src_path} to {dst_path}')
                    blob_client.upload_blob(
                        data=f,
                        overwrite=overwrite,
                        content_settings=ContentSettings(content_type=content_type),
                        progress_hook=_progress_,
                        max_concurrency=max_concurrency
                    )
            elif data:
                logger.debug(f'Uploading bytes/data to {dst_path}')
                blob_client.upload_blob(
                    data=data,
                    overwrite=overwrite,
                    content_settings=ContentSettings(content_type=content_type),
                    progress_hook=_progress_,
                    max_concurrency=max_concurrency
                )
            else:
                raise ValueError("Either 'src_path' or 'data' must be provided.")
        except Exception as e:
            raise e

    async def download(
            self,
            blob_name: str = None,
            dst_path: str = None,
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
        try:
            logger.debug(f'Downloading {blob_name}')
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            chunk_list = []
            stream = await blob_client.download_blob()
            async for chunk in stream.chunks():
                chunk_list.append(chunk)

            data = b"".join(chunk_list)
            logger.debug(f'Finished downloading {blob_name}')
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
        destination_blob = self.container_client.get_blob_client(blob=destination_blob_name)
        return await destination_blob.start_copy_from_url(source_blob.url)

    async def list_base_files(self, indicator_ids=None):
        """
        List
        :param indicator_id:
        :return:
        """
        return [blob.name async for blob in self.container_client.list_blobs(
            name_starts_with=os.path.join(self.OUTPUT_PATH, 'access_all_data', 'base/'))]

    async def upload_cfg(self, cfg_dict=None, cfg_path=None):
        """
        Upload a config file to azure storage
        :param cfg_dict:
        :param cfg_path:
        :return:
        """
        parser = dict2cfg(cfg_dict=cfg_dict)
        content = parser.write_string()
        content_bytes = content.encode('utf-8')
        await self.upload(dst_path=cfg_path, data=content_bytes, content_type='text/plain')

    async def list_blobs(self, prefix=None):
        """
        List
        :param prefix:
        :return:
        """
        return [blob.name async for blob in self.container_client.list_blobs(name_starts_with=prefix)]

    async def cached_download(self, source_path=None, chunked=False):
        logger.debug(f"Downloading {source_path}")
        if source_path in TMP_SOURCES:
            cached_src_path = TMP_SOURCES[source_path]
            logger.info(f'Rereading cached {source_path} from {cached_src_path} ')
            data = open(cached_src_path, 'rb').read()
        else:

            # check the blob exists in azure storage
            source_path_exists = await self.check_blob_exists(blob_name=source_path)
            logger.debug(f"Checking if {source_path} exists {source_path_exists}")
            if not source_path_exists:
                raise Exception(f'Source {source_path} does not exist')
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

            cached = tempfile.NamedTemporaryFile(mode='wb', delete=False)
            logger.debug(f'Going to cache {source_path} to {cached.name}')
            cached.write(data)
            TMP_SOURCES[source_path] = cached.name
        return data
