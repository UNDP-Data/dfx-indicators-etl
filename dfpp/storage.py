import asyncio
import configparser
import os
from typing import Any, AsyncGenerator, Dict, List, Tuple, Type

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

    @staticmethod
    def create_instance(connection_string: str = None, container_name: str = None):
        """
        Returns a singleton instance of the class.
        """
        if AzureBlobStorageManager._instance is None:
            AzureBlobStorageManager._instance = AzureBlobStorageManager(
                connection_string=connection_string, container_name=container_name
            )
        return AzureBlobStorageManager._instance

    def __init__(self, connection_string: str = None, container_name: str = None):
        """
        Initializes the container client for Azure Blob Storage.
        """
        if AzureBlobStorageManager._instance is not None:
            raise Exception(
                "Use create_instance() to get a singleton instance of this class"
            )
        else:
            self.container_client = ContainerClient.from_connection_string(
                connection_string=connection_string, container_name=container_name
            )

    def list_blobs(self):
        blob_list = []
        for blob in self.container_client.list_blobs():
            blob_list.append(blob)
        return blob_list

    def list_and_filter(self, prefix: str = None):
        filtered_blobs = []
        for blob in self.container_client.list_blobs(name_starts_with=prefix):
            filtered_blobs.append(blob)
        return filtered_blobs

    def hierarchical_list(self, delimiter="/"):
        hierarchical_blobs = []
        for blob in self.container_client.walk_blobs(delimiter=delimiter):
            hierarchical_blobs.append(blob)
        return hierarchical_blobs

    def delete(self, blob_name: str = None):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        blob_client.delete_blob()

    def download(self, blob_name: str = None, file_path: str = None):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        with open(file_path, "wb") as f:
            data = blob_client.download_blob()
            f.write(data.readall())

    def upload(self, blob_name: str = None, file_path: str = None):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        with open(file_path, "rb") as f:
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

    @classmethod
    async def create_instance(
        cls: Type[AsyncAzureBlobStorageManager],
        connection_string: str = None,
        container_name: str = None,
    ):
        """
        Asynchronously creates and initializes an instance of the AsyncAzureBlobStorageManager class
        using the Singleton pattern. If an instance already exists, it returns the existing instance.

        :param cls: A singleton class instance of AsyncAzureBlobStorageManager.
        :type: Type[AsyncAzureBlobStorageManager]
        :param connection_string: The connection string for the Azure Blob Storage account.
        :type connection_string: str
        :param container_name: The name of the container to be used in Azure Blob Storage.
        :type container_name: str
        :return: An initialized instance of the AsyncAzureBlobStorageManager class.
        :rtype: AsyncAzureBlobStorageManager
        """
        if cls._instance is None:
            container_client = AContainerClient.from_connection_string(
                connection_string=connection_string, container_name=container_name
            )
            await cls._initialize(container_client=container_client)
            cls._instance = cls(container_client)
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

    async def list_blobs(self):
        blob_list = []
        async for blob in self.container_client.list_blobs():
            blob_list.append(blob)
        return blob_list

    async def list_and_filter(self, prefix: str = None):
        filtered_blobs = []
        async for blob in self.container_client.list_blobs(name_starts_with=prefix):
            filtered_blobs.append(blob)
        return filtered_blobs

    async def hierarchical_list(self, delimiter: str = "/"):
        hierarchical_blobs = []
        async for blob in self.container_client.walk_blobs(delimiter=delimiter):
            hierarchical_blobs.append(blob)
        return hierarchical_blobs

    async def list_sources(
        self, root_folder: str, delimiter: str = "/"
    ) -> Dict[str, Dict[str, Any]]:
        """
        Asynchronously lists source configuration files from an Azure Blob Container,
        and returns a dictionary containing their parsed content.

        Args:
            root_folder (str): The root folder for the query.
            delimiter (str, optional): The delimiter used for the Azure Blob storage paths. Defaults to "/".

        Returns:
            Dict[str, Dict[str, Any]]: A dictionary where keys are source IDs and values are dictionaries
                                        containing the key-value pairs of the source configuration.

        Raises:
            ConfigError: Raised if a source configuration file is invalid or has a missing "source" section.
        """
        cfg = {}
        prefix = os.path.join(root_folder, "config", "sources")
        async for blob in self.container_client.walk_blobs(
            name_starts_with=prefix, delimiter=delimiter
        ):
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
                else:
                    raise ConfigError(f"Invalid source")
        return cfg

    async def list_source_indicators(
        self, root_folder: str, cfg: Dict[str, Dict[str, Any]], delimiter: str = "/"
    ) -> Dict[str, Dict[str, Any]]:
        """
        Asynchronously lists source indicator configuration files from an Azure Blob Container,
        and updates the provided 'cfg' dictionary with the parsed content.

        Args:
            root_folder (str): The root folder for the query.
            cfg (Dict[str, Dict[str, Any]]): A dictionary containing the source configuration information.
            delimiter (str, optional): The delimiter used for the Azure Blob storage paths. Defaults to "/".

        Returns:
            Dict[str, Dict[str, Any]]: The updated 'cfg' dictionary with the source indicator information
                                        (keys are source IDs, and values are dictionaries containing source
                                        configuration and a list of associated indicator IDs).

        Raises:
            ConfigError: Raised if an indicator configuration file is invalid or has a missing "indicator" section.
        """
        prefix = os.path.join(root_folder, "config", "sources", "indicators")
        async for blob in self.container_client.walk_blobs(
            name_starts_with=prefix, delimiter=delimiter
        ):
            if not isinstance(blob, BlobPrefix) and blob.name.endswith(".cfg"):
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

    async def get_utilities(
        self, root_folder: str, utility_file: str, delimiter: str = "/"
    ) -> Dict[str, Dict[str, Any]]:
        """
        Asynchronously retrieves a specified utility configuration file from the Azure Blob Container,
        parses it, and returns its content as a dictionary.

        Args:
            root_folder (str): The root folder for the query.
            utility_file (str): The name of the utility configuration file to search for.
            delimiter (str, optional): The delimiter used for the Azure Blob storage paths. Defaults to "/".

        Returns:
            dict: A dictionary representation of the utility configuration file content,
                where keys are section names and values are dictionaries of key-value pairs within the section.

        Raises:
            ConfigError: Raised if the specified utility configuration file is not found or not valid.

        """
        prefix = os.path.join(root_folder, "config", "utilities")
        async for blob in self.container_client.walk_blobs(
            name_starts_with=prefix, delimiter=delimiter
        ):
            if (
                not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".cfg")
                and utility_file in blob.name
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
        root_folder: str,
        source_type: str,
        delimiter: str = "/",
        source_query: List[str] = None,
    ) -> AsyncGenerator[bytes, None]:
        """
        Asynchronously queries an Azure Blob Container for CSV files in a specific directory,
        and yields them one by one as a generator.

        Args:
            root_folder (str): The root folder for the query.
            source_type (str): The subdirectory under 'sources' to search for CSV files.
                            Values can either be "raw" or "standardized"
            delimiter (str, optional): The delimiter used for the Azure Blob storage paths. Defaults to "/".
            source_query (list, optional): A list of source ids to search for. If empty or None,
                                        all CSV files in the directory will be returned. Defaults to None.

        Yields:
            bytes: The content of a CSV file in bytes.

        Raises:
            DFPSourceError: Raised if no matching CSV files are found in the specified directory.

        Example usage:
            async for dataset in AsyncAzureBlobStorageManager.get_source_files(root_folder, source_type, delimiter, source_query):
                # Open and process each dataset here
        """
        if source_type not in ("raw", "standardized"):
            raise ValueError("source_type must be either 'raw' or 'standardized'")

        if source_query is None:
            source_query = []

        prefix = os.path.join(root_folder, "sources", source_type)
        found_csv = False
        async for blob in self.container_client.walk_blobs(
            name_starts_with=prefix, delimiter=delimiter
        ):
            if len(source_query) > 0:
                for source_id in source_query:
                    if (
                        not isinstance(blob, BlobPrefix)
                        and blob.name.endswith(".csv")
                        and source_id in blob.name
                    ):
                        stream = await self.container_client.download_blob(
                            blob.name, max_concurrency=8
                        )
                        content = await stream.readall()
                        found_csv = True
                        yield content
            elif (
                not source_query
                and not isinstance(blob, BlobPrefix)
                and blob.name.endswith(".csv")
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
        self, subfolder: str, delimiter: str = "/"
    ) -> AsyncGenerator[Tuple[str, bytes], None]:
        # ...
        """
        Asynchronously retrieves the contents of CSV and JSON files within the "raw" folder
        inside the specified subfolder directory.

        Args:
            subfolder (str): The subdirectory under 'output' to search for files.
                                Values can either be "access_all_data" or "vaccine_equity"
            delimiter (str, optional): The delimiter used for the Azure Blob storage paths. Defaults to "/".

        Yields:
            Tuple[str, bytes]: A tuple containing the file name and the content of the file in bytes.

        Raises:
            ValueError: Raised if subfolder is not "access_all_data" or "vaccine_equity".
        """

        if subfolder not in ("access_all_data", "vaccine_equity"):
            raise ValueError(
                "output_type must be either 'access_all_data' or 'vaccine_equity'"
            )

        prefix = os.path.join("output", subfolder, "raw")
        async for blob in self.container_client.walk_blobs(
            name_starts_with=prefix, delimiter=delimiter
        ):
            if not isinstance(blob, BlobPrefix):
                stream = await self.container_client.download_blob(
                    blob.name, max_concurrency=8
                )
                content = await stream.readall()
                yield blob.name, content

    async def delete(self, blob_name: str = None) -> None:
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        await blob_client.delete_blob()

    async def download(self, blob_name: str = None, file_path: str = None) -> None:
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        with open(file_path, "wb") as f:
            data = await blob_client.download_blob()
            f.write(await data.readall())

    async def upload(self, blob_name: str = None, file_path: str = None) -> None:
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        with open(file_path, "rb") as f:
            await blob_client.upload_blob(data=f)


async def main():
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    container_name = os.environ["CONTAINER_NAME"]
    root_folder = os.environ["ROOT_FOLDER"]

    async_manager = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=connection_string, container_name=container_name
    )
    # perform other asynchronous operations with async_manager here
    await async_manager.list_source_config()
    await async_manager.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
