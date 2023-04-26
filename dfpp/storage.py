import asyncio

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import ContainerClient
from azure.storage.blob.aio import ContainerClient as AContainerClient


class AzureBlobStorageManager:
    """
    A class to manage Azure Blob Storage operations.
    Implements singleton pattern to ensure only one instance is created.
    """

    _instance = None

    @staticmethod
    def get_instance(connection_string, container_name):
        """
        Returns a singleton instance of the class.
        """
        if AzureBlobStorageManager._instance is None:
            AzureBlobStorageManager._instance = AzureBlobStorageManager(
                connection_string, container_name
            )
        return AzureBlobStorageManager._instance

    def __init__(self, connection_string, container_name):
        """
        Initializes the container client for Azure Blob Storage.
        """
        if AzureBlobStorageManager._instance is not None:
            raise Exception(
                "Use get_instance() to get a singleton instance of this class"
            )
        else:
            self.container_client = ContainerClient.from_connection_string(
                connection_string, container_name
            )
            try:
                self.container_client.create_container()
            except ResourceExistsError:
                pass

    def list_blobs(self):
        blob_list = []
        for blob in self.container_client.list_blobs():
            blob_list.append(blob)
        return blob_list

    def list_and_filter(self, prefix):
        filtered_blobs = []
        for blob in self.container_client.list_blobs(name_starts_with=prefix):
            filtered_blobs.append(blob)
        return filtered_blobs

    def hierarchical_list(self, delimiter="/"):
        hierarchical_blobs = []
        for blob in self.container_client.walk_blobs(delimiter=delimiter):
            hierarchical_blobs.append(blob)
        return hierarchical_blobs

    def delete(self, blob_name):
        blob_client = self.container_client.get_blob_client(blob_name)
        blob_client.delete_blob()

    def download(self, blob_name, file_path, buffer_size=8 * 1024 * 1024):
        blob_client = self.container_client.get_blob_client(blob_name)
        data = blob_client.download_blob()
        with open(file_path, "wb") as f:
            for chunk in data.chunks(buffer_size):
                f.write(chunk)

    def upload(self, blob_name, file_path, buffer_size=8 * 1024 * 1024):
        blob_client = self.container_client.get_blob_client(blob_name)
        with open(file_path, "rb") as f:
            data = f.read(buffer_size)
            offset = 0
            while data:
                blob_client.upload_blob(
                    data,
                    blob_type="BlockBlob",
                    length=len(data),
                    overwrite=True,
                    offset=offset,
                )
                offset += len(data)
                data = f.read(buffer_size)


class AsyncAzureBlobStorageManager:
    """
    An asynchronous class to manage Azure Blob Storage operations.
    Implements singleton pattern to ensure only one instance is created.
    """

    _instance = None

    @staticmethod
    async def get_instance(connection_string, container_name):
        """
        Returns a singleton instance of the class.
        """
        if AsyncAzureBlobStorageManager._instance is None:
            AsyncAzureBlobStorageManager._instance = AsyncAzureBlobStorageManager(
                connection_string, container_name
            )
        return AsyncAzureBlobStorageManager._instance

    def __init__(self, connection_string, container_name):
        """
        Initializes the container client for Azure Blob Storage.
        """
        if AsyncAzureBlobStorageManager._instance is not None:
            raise Exception(
                "Use get_instance() to get a singleton instance of this class"
            )
        else:
            self.connection_string = connection_string
            self.container_name = container_name
            asyncio.run(self._initialize())

    async def _initialize(self):
        """
        A private method to create the container client for Azure Blob Storage.
        """
        self.container_client = AContainerClient.from_connection_string(
            self.connection_string, self.container_name
        )
        try:
            await self.container_client.create_container()
        except ResourceExistsError:
            pass

    async def list_blobs(self):
        blob_list = []
        async for blob in self.container_client.list_blobs():
            blob_list.append(blob)
        return blob_list

    async def list_and_filter(self, prefix):
        filtered_blobs = []
        async for blob in self.container_client.list_blobs(name_starts_with=prefix):
            filtered_blobs.append(blob)
        return filtered_blobs

    async def hierarchical_list(self, delimiter="/"):
        hierarchical_blobs = []
        async for blob in self.container_client.walk_blobs(delimiter=delimiter):
            hierarchical_blobs.append(blob)
        return hierarchical_blobs

    async def delete(self, blob_name):
        blob_client = self.container_client.get_blob_client(blob_name)
        await blob_client.delete_blob()

    async def download(self, blob_name, file_path, buffer_size=8 * 1024 * 1024):
        blob_client = self.container_client.get_blob_client(blob_name)
        data = await blob_client.download_blob()
        with open(file_path, "wb") as f:
            async for chunk in data.chunks(buffer_size):
                f.write(chunk)

    async def upload(self, blob_name, file_path, buffer_size=8 * 1024 * 1024):
        blob_client = self.container_client.get_blob_client(blob_name)
        with open(file_path, "rb") as f:
            data = f.read(buffer_size)
            offset = 0
            while data:
                await blob_client.upload_blob(
                    data,
                    blob_type="BlockBlob",
                    length=len(data),
                    overwrite=True,
                    offset=offset,
                )
                offset += len(data)
                data = f.read(buffer_size)
