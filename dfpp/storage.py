from typing import Type

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient
from azure.storage.blob.aio import ContainerClient as AContainerClient


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
    
    async def list_sources(self):
        

    async def list_and_filter(self, prefix: str = None):
        filtered_blobs = []
        async for blob in self.container_client.list_blobs(name_starts_with=prefix):
            filtered_blobs.append(blob)
        return filtered_blobs

    async def hierarchical_list(self, delimiter="/"):
        hierarchical_blobs = []
        async for blob in self.container_client.walk_blobs(delimiter=delimiter):
            hierarchical_blobs.append(blob)
        return hierarchical_blobs

    async def delete(self, blob_name: str = None):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        await blob_client.delete_blob()

    async def download(self, blob_name: str = None, file_path: str = None):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        with open(file_path, "wb") as f:
            data = await blob_client.download_blob()
            f.write(await data.readall())

    async def upload(self, blob_name: str = None, file_path: str = None):
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        with open(file_path, "rb") as f:
            await blob_client.upload_blob(data=f)
