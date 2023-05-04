import asyncio
import configparser
import os
from typing import Any, AsyncGenerator, Dict, Generator, List, Tuple

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient
from azure.storage.blob.aio import BlobPrefix
from azure.storage.blob.aio import ContainerClient as AContainerClient

from dfpp.dfpp_exceptions import ConfigError, DFPSourceError


class AzureBlobStorageManager:
    """
    A class to manage Azure Blob Storage operations.
    Implements singleton pattern to ensure only one instance is created.
    """

    _instance = None

    def __init__(self, connection_string: str = None, container_name: str = None):
        """
        Initializes the container client for Azure Blob Storage.
        """
        self.delimiter = "/"
        self.ROOT_FOLDER = os.environ["ROOT_FOLDER"]

        if AzureBlobStorageManager._instance is not None:
            raise Exception(
                "Use create_instance() to get a singleton instance of this class"
            )
        else:
            self.container_client = ContainerClient.from_connection_string(
                conn_str=connection_string, container_name=container_name
            )

    @staticmethod
    def create_instance(
        connection_string: str = None,
        container_name: str = None,
        use_singleton: bool = True,
    ):
        """
        Creates and initializes an instance of the AzureBlobStorageManager class
        using the Singleton pattern. If an instance already exists and use_singleton is True,
        it returns the existing instance. If use_singleton is False, it always creates a new instance.

        :param connection_string: The connection string for the Azure Blob Storage account.
        :type connection_string: str
        :param container_name: The name of the container to be used in Azure Blob Storage.
        :type container_name: str
        :param use_singleton: A boolean to determine if the Singleton pattern should be used.
                            If True, return the existing instance if available. If False, create a new instance.
        :type use_singleton: bool, optional, default=True
        :return: An initialized instance of the AzureBlobStorageManager class.
        :rtype: AzureBlobStorageManager
        """
        if use_singleton and AzureBlobStorageManager._instance is not None:
            return AzureBlobStorageManager._instance
        else:
            AzureBlobStorageManager._instance = AzureBlobStorageManager(
                connection_string=connection_string, container_name=container_name
            )
            return AzureBlobStorageManager._instance

    def hierarchical_list(self, prefix: str = None):
        for blob in self.container_client.walk_blobs(
            name_starts_with=prefix, delimiter=self.delimiter
        ):
            yield blob

    def _yield_blobs(self, prefix: str = None):
        for blob in self.container_client.list_blobs(name_starts_with=prefix):
            yield blob

    def list_blobs(self, prefix: str = None):
        blob_list = []
        for blob in self.container_client.list_blobs(name_starts_with=prefix):
            blob_list.append(blob)
        return blob_list

    def list_and_filter(self, prefix: str = None):
        filtered_blobs = []
        for blob in self.container_client.list_blobs(name_starts_with=prefix):
            filtered_blobs.append(blob)
        return filtered_blobs

    def list_indicators(self) -> List[str]:
        """
        Synchronously lists indicator configuration files from an Azure Blob Container,
        and returns a list containing all of the indicator_ids present in the directory.

        Returns:
            List[str]: A list where each value is an indicator_id taken from individual indicator
                                    configuration files.

        Raises:
            ConfigError: Raised if an indicator configuration file is invalid or has a missing "indicator" section.
        """
        indicator_list = []
        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/config/sources"
        for blob in self._yield_blobs(prefix=prefix):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and "indicators" in blob.name
            ):
                stream = self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if "indicator" in parser:
                    indicator_list.append(parser["indicator"].get("id"))

                else:
                    raise ConfigError(f"Invalid indicator config")
        return indicator_list

    def list_sources(self) -> List[str]:
        """
        Synchronously lists source configuration files from an Azure Blob Container,
        and returns a list containing all of the source_ids present in the directory.

        Returns:
            List[str]: A list where each value is a source_id taken from individual source
                                        configuration files.

        Raises:
            ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
        """
        source_list = []
        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/config/sources"
        for blob in self._yield_blobs(prefix=prefix):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and not "indicators" in blob.name
            ):
                stream = self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if "source" in parser:
                    source_list.append(parser["source"].get("id"))
                else:
                    raise ConfigError(f"Invalid source")
        return source_list

    def get_source_config(self) -> Dict[str, Dict[str, Any]]:
        """
        Synchronously lists source configuration files from an Azure Blob Container,
        and returns a dictionary containing their parsed content.

        Returns:
            Dict[str, Dict[str, Any]]: A dictionary where keys are source IDs and values are dictionaries
                                        containing the key-value pairs of the source configuration.

        Raises:
            ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
        """
        cfg = {}
        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/config/sources"
        for blob in self._yield_blobs(prefix=prefix):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and not "indicators" in blob.name
            ):
                stream = self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if "source" in parser:
                    src_id = parser["source"].get("id")
                    cfg[src_id] = dict(parser["source"].items())
                    if "downloader_function_args" in parser:
                        cfg[src_id].update(
                            dict(parser["downloader_function_args"].items())
                        )
                else:
                    raise ConfigError(f"Invalid source")
        return cfg

    def get_source_indicator_config(self) -> Dict[str, Dict[str, Any]]:
        """
        Synchronously lists source indicator configuration files from an Azure Blob Container,
        and updates the provided 'cfg' dictionary with the parsed content.

        Returns:
            Dict[str, Dict[str, Any]]: The updated 'cfg' dictionary with the source indicator information
                                        (keys are source IDs, and values are dictionaries containing source
                                        configuration and a list of associated indicator IDs).

        Raises:
            ConfigError: Raised if an indicator configuration file is invalid or has a missing "indicator" section.
        """
        cfg = self.get_source_config()
        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/config/sources"
        for blob in self._yield_blobs(prefix=prefix):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and "indicators" in blob.name
            ):
                stream = self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if "indicator" in parser:
                    src_id = parser["indicator"].get("source_id")
                    indicator_id = parser["indicator"].get("id")
                    # set indicator value to a list if it does not exist yet, and append the value to the existing/new list
                    cfg[src_id].setdefault("indicators", []).append(indicator_id)
                else:
                    raise ConfigError(f"Invalid indicator config")
        return cfg

    def get_utility_file(self, utility_file: str) -> Dict[str, Dict[str, Any]]:
        """
        Synchronously retrieves a specified utility configuration file from the Azure Blob Container,
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
        prefix = f"{self.ROOT_FOLDER}/config/utilities"
        for blob in self._yield_blobs(prefix=prefix):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and utility_file == os.path.basename(blob.name)
            ):
                stream = self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                config_dict = {
                    section: dict(parser.items(section))
                    for section in parser.sections()
                }
                return config_dict
        else:
            raise ConfigError(f"Utitlity source not valid")

    def get_source_files(
        self,
        source_type: str,
        source_files: List[str] = None,
    ) -> Generator[bytes, None, None]:
        """
        Synchronously queries an Azure Blob Container for CSV files in a specific directory,
        and yields them one by one as a generator.

        Args:
            source_type (str): The subdirectory under 'sources' to search for CSV files.
                            Values can either be "raw" or "standardized"
            source_files (list, optional): A list of source files to search for. If empty or None,
                                        all CSV files in the directory will be returned. Defaults to None.

        Yields:
            bytes: The content of a CSV file in bytes.

        Raises:
            DFPSourceError: Raised if no matching CSV files are found in the specified directory.

        Example usage:
            for dataset in AzureBlobStorageManager.get_source_files(source_type, source_files):
                # Open and process each dataset here
        """
        if source_type not in ("raw", "standardized"):
            raise ValueError("source_type must be either 'raw' or 'standardized'")

        if source_files is None:
            source_files = []

        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/sources/{source_type}"
        found_csv = False
        for blob in self._yield_blobs(prefix=prefix):
            if len(source_files) > 0:
                for source_id in source_files:
                    if (
                        not isinstance(blob, BlobPrefix)
                        and (
                            blob.name.endswith(".csv")
                            or blob.name.endswith(".xlsx")
                            or blob.name.endswith(".xls")
                        )
                        and source_id == os.path.basename(blob.name)
                    ):
                        stream = self.container_client.download_blob(
                            blob.name, max_concurrency=8
                        )
                        content = stream.readall()
                        found_csv = True
                        yield content
            elif not isinstance(blob, BlobPrefix) and (
                blob.name.endswith(".csv")
                or blob.name.endswith(".xlsx")
                or blob.name.endswith(".xls")
            ):
                stream = self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = stream.readall()
                found_csv = True
                yield content

        if not found_csv:
            raise DFPSourceError(
                f"An error occurred returning the source CSVs from the raw directory."
            )

    def get_output_files(
        self, subfolder: str
    ) -> Generator[Tuple[str, bytes], None, None]:
        # ...
        """
        Synchronously retrieves the contents of CSV and JSON files within the "raw" folder
        inside the specified subfolder directory.

        Args:
            subfolder (str): The subdirectory under 'output' to search for files.
                                Values can either be "access_all_data" or "vaccine_equity"

        Yields:
            Tuple[str, bytes]: A tuple containing the file name and the content of the file in bytes.

        Raises:
            ValueError: Raised if subfolder is not "access_all_data" or "vaccine_equity".
        """

        if subfolder not in ("access_all_data", "vaccine_equity"):
            raise ValueError(
                "output_type must be either 'access_all_data' or 'vaccine_equity'"
            )

        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/output/{subfolder}/raw"
        for blob in self._yield_blobs(prefix=prefix):
            if not isinstance(blob, BlobPrefix):
                stream = self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = stream.readall()
                yield blob.name, content

    def delete(self, blob_name: str = None) -> None:
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        blob_client.delete_blob()

    def download(self, blob_name: str = None, dst_path: str = None) -> None:
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        with open(dst_path, "wb") as f:
            data = blob_client.download_blob()
            f.write(data.readall())

    def upload(self, dst_path: str = None, src_path: str = None) -> None:
        blob_client = self.container_client.get_blob_client(blob=dst_path)
        with open(src_path, "rb") as f:
            blob_client.upload_blob(data=f)


class AsyncAzureBlobStorageManager:
    """
    An asynchronous class to manage Azure Blob Storage operations.
    Implements singleton pattern to ensure only one instance is created.
    """

    _instance = None

    def __init__(self, container_client):
        """
        Initializes a new instance of the AsyncAzureBlobStorageManager class.

        Note: This method should not be called directly. Use the create_instance() method instead.

        :param container_client: An instance of the ContainerClient class.
        :type container_client: ContainerClient
        """
        self.container_client = container_client
        self.delimiter = "/"
        self.ROOT_FOLDER = os.environ["ROOT_FOLDER"]

    @classmethod
    async def create_instance(
        cls: "AsyncAzureBlobStorageManager",
        connection_string: str = None,
        container_name: str = None,
        use_singleton: bool = True,
    ):
        """
        Asynchronously creates and initializes an instance of the AsyncAzureBlobStorageManager class
        using the Singleton pattern. If an instance already exists and use_singleton is True,
        it returns the existing instance. If use_singleton is False, it always creates a new instance.

        :param cls: A singleton class instance of AsyncAzureBlobStorageManager.
        :type: Type[AsyncAzureBlobStorageManager]
        :param connection_string: The connection string for the Azure Blob Storage account.
        :type connection_string: str
        :param container_name: The name of the container to be used in Azure Blob Storage.
        :type container_name: str
        :param use_singleton: A boolean to determine if the Singleton pattern should be used.
                            If True, return the existing instance if available. If False, create a new instance.
        :type use_singleton: bool, optional, default=True
        :return: An initialized instance of the AsyncAzureBlobStorageManager class.
        :rtype: AsyncAzureBlobStorageManager
        """
        if use_singleton:
            if cls._instance is None:
                # Create an instance if it doesn't exist
                container_client = AContainerClient.from_connection_string(
                    conn_str=connection_string, container_name=container_name
                )
                cls._instance = cls(container_client)
            return cls._instance
        else:
            # Always create a new instance
            container_client = AContainerClient.from_connection_string(
                conn_str=connection_string, container_name=container_name
            )
            return cls(container_client)

    @staticmethod
    async def _initialize(container_client):
        """
        Asynchronously checks if the Azure Blob Storage container exists.

        :param container_client: An instance of the ContainerClient class.
        :type container_client: ContainerClient
        :raises ResourceNotFoundError: If the container does not exist.
        """
        try:
            await container_client.get_container_properties()
        except ResourceNotFoundError as e:
            raise ResourceNotFoundError(
                f"The container '{container_client.container_name}' does not exist."
            ) from e

    async def hierarchical_list(self, prefix: str = None):
        async for blob in self.container_client.walk_blobs(
            name_starts_with=prefix, delimiter=self.delimiter
        ):
            yield blob

    async def list_blobs(self, prefix: str = None):
        blob_list = []
        async for blob in self.container_client.list_blobs(name_starts_with=prefix):
            blob_list.append(blob)
        return blob_list

    async def _yield_blobs(self, prefix: str = None):
        async for blob in self.container_client.list_blobs(name_starts_with=prefix):
            yield blob

    async def list_and_filter(self, prefix: str = None, filter: str = None):
        filtered_blobs = []
        name_starts_with = f"{prefix}/{filter}"
        async for blob in self.container_client.list_blobs(
            name_starts_with=name_starts_with
        ):
            filtered_blobs.append(blob)
        return filtered_blobs

    async def list_indicators(self) -> List[str]:
        """
        Asynchronously lists indicator configuration files from an Azure Blob Container,
        and returns a list containing all of the indicator_ids present in the directory.

        Returns:
            List[str]: A list where each value is an indicator_id taken from individual indicator
                                    configuration files.

        Raises:
            ConfigError: Raised if an indicator configuration file is invalid or has a missing "indicator" section.
        """
        indicator_list = []
        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/config/sources"
        async for blob in self._yield_blobs(prefix=prefix):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and "indicators" in blob.name
            ):
                stream = await self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = await stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if "indicator" in parser:
                    indicator_list.append(parser["indicator"].get("id"))

                else:
                    raise ConfigError(f"Invalid indicator config")
        return indicator_list

    async def list_sources(self) -> List[str]:
        """
        Asynchronously lists source configuration files from an Azure Blob Container,
        and returns a list containing all of the source_ids present in the directory.

        Returns:
            List[str]: A list where each value is a source_id taken from individual source
                                        configuration files.

        Raises:
            ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
        """
        source_list = []
        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/config/sources"
        async for blob in self._yield_blobs(prefix=prefix):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and not "indicators" in blob.name
            ):
                stream = await self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = await stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if "source" in parser:
                    source_list.append(parser["source"].get("id"))
                else:
                    raise ConfigError(f"Invalid source")
        return source_list

    async def get_source_config(self) -> Dict[str, Dict[str, Any]]:
        """
        Asynchronously lists source configuration files from an Azure Blob Container,
        and returns a dictionary containing their parsed content.

        Returns:
            Dict[str, Dict[str, Any]]: A dictionary where keys are source IDs and values are dictionaries
                                        containing the key-value pairs of the source configuration.

        Raises:
            ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
        """
        cfg = {}
        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/config/sources"
        async for blob in self._yield_blobs(prefix=prefix):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and not "indicators" in blob.name
            ):
                stream = await self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = await stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if "source" in parser:
                    src_id = parser["source"].get("id")
                    cfg[src_id] = dict(parser["source"].items())
                    if "downloader_function_args" in parser:
                        cfg[src_id].update(
                            dict(parser["downloader_function_args"].items())
                        )
                else:
                    raise ConfigError(f"Invalid source")
        return cfg

    async def get_source_indicator_config(self) -> Dict[str, Dict[str, Any]]:
        """
        Asynchronously lists source indicator configuration files from an Azure Blob Container,
        and updates the provided 'cfg' dictionary with the parsed content.

        Returns:
            Dict[str, Dict[str, Any]]: The updated 'cfg' dictionary with the source indicator information
                                        (keys are source IDs, and values are dictionaries containing source
                                        configuration and a list of associated indicator IDs).

        Raises:
            ConfigError: Raised if an indicator configuration file is invalid or has a missing "indicator" section.
        """
        cfg = await self.get_source_config()
        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/config/sources"
        async for blob in self._yield_blobs(prefix=prefix):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and "indicators" in blob.name
            ):
                stream = await self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = await stream.readall()
                content_str = content.decode("utf-8")
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                if "indicator" in parser:
                    src_id = parser["indicator"].get("source_id")
                    indicator_id = parser["indicator"].get("id")
                    # set indicator value to a list if it does not exist yet, and append the value to the existing/new list
                    cfg[src_id].setdefault("indicators", []).append(indicator_id)
                else:
                    raise ConfigError(f"Invalid indicator config")
        return cfg

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
        prefix = f"{self.ROOT_FOLDER}/config/utilities"
        async for blob in self._yield_blobs(prefix=prefix):
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
                parser = configparser.ConfigParser()
                parser.read_string(content_str)
                config_dict = {
                    section: dict(parser.items(section))
                    for section in parser.sections()
                }
                return config_dict
        else:
            raise ConfigError(f"Utitlity source not valid")

    async def get_source_files(
        self,
        source_type: str,
        source_files: List[str] = None,
    ) -> AsyncGenerator[bytes, None]:
        """
        Asynchronously queries an Azure Blob Container for CSV files in a specific directory,
        and yields them one by one as a generator.

        Args:
            source_type (str): The subdirectory under 'sources' to search for CSV files.
                            Values can either be "raw" or "standardized"
            source_files (list, optional): A list of source files to search for. If empty or None,
                                        all CSV files in the directory will be returned. Defaults to None.

        Yields:
            bytes: The content of a CSV file in bytes.

        Raises:
            DFPSourceError: Raised if no matching CSV files are found in the specified directory.

        Example usage:
            async for dataset in AsyncAzureBlobStorageManager.get_source_files(source_type, source_files):
                # Open and process each dataset here
        """
        if source_type not in ("raw", "standardized"):
            raise ValueError("source_type must be either 'raw' or 'standardized'")

        if source_files is None:
            source_files = []

        found_csv = False
        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/sources/{source_type}"
        async for blob in self._yield_blobs(prefix=prefix):
            if len(source_files) > 0:
                for source_id in source_files:
                    if (
                        not isinstance(blob, BlobPrefix)
                        and (
                            blob.name.endswith(".csv")
                            or blob.name.endswith(".xlsx")
                            or blob.name.endswith(".xls")
                        )
                        and source_id == os.path.basename(blob.name)
                    ):
                        stream = await self.container_client.download_blob(
                            blob.name, max_concurrency=8
                        )
                        content = await stream.readall()
                        found_csv = True
                        yield content
            elif not isinstance(blob, BlobPrefix) and (
                blob.name.endswith(".csv")
                or blob.name.endswith(".xlsx")
                or blob.name.endswith(".xls")
            ):
                stream = await self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = await stream.readall()
                found_csv = True
                yield content

        if not found_csv:
            raise DFPSourceError(
                f"An error occurred returning the source CSVs from the raw directory."
            )

    async def get_output_files(
        self, subfolder: str
    ) -> AsyncGenerator[Tuple[str, bytes], None]:
        # ...
        """
        Asynchronously retrieves the contents of CSV and JSON files within the "raw" folder
        inside the specified subfolder directory.

        Args:
            subfolder (str): The subdirectory under 'output' to search for files.
                                Values can either be "access_all_data" or "vaccine_equity"

        Yields:
            Tuple[str, bytes]: A tuple containing the file name and the content of the file in bytes.

        Raises:
            ValueError: Raised if subfolder is not "access_all_data" or "vaccine_equity".
        """

        if subfolder not in ("access_all_data", "vaccine_equity"):
            raise ValueError(
                "output_type must be either 'access_all_data' or 'vaccine_equity'"
            )

        # os.path.join doesn't work for filtering returned Azure blob paths
        prefix = f"{self.ROOT_FOLDER}/output/{subfolder}/raw"
        async for blob in self._yield_blobs(prefix=prefix):
            if not isinstance(blob, BlobPrefix):
                stream = await self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = await stream.readall()
                yield blob.name, content

    async def delete(self, blob_name: str = None) -> None:
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        await blob_client.delete_blob()

    async def download(self, blob_name: str = None, dst_path: str = None) -> None:
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        with open(dst_path, "wb") as f:
            data = await blob_client.download_blob()
            f.write(await data.readall())

    async def upload(self, dst_path: str = None, src_path: str = None) -> None:
        blob_client = self.container_client.get_blob_client(blob=dst_path)
        with open(src_path, "rb") as f:
            await blob_client.upload_blob(data=f)
