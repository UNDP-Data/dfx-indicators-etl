import os
import unittest

from ..storage import AsyncAzureBlobStorageManager, AzureBlobStorageManager

CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]


class TestSyncAzureBlobStorageManager(unittest.TestCase):
    # Test the synchronous class
    def test_sync_class(self):
        manager = AzureBlobStorageManager.create_instance(
            CONNECTION_STRING, CONTAINER_NAME
        )

        # Create a test file
        with open("sync_test.txt", "w") as f:
            f.write("This is a test file")

        # Upload the test file
        manager.upload("sync_test.txt", "sync_test.txt")

        # List blobs
        print("Blobs in the container:")
        for blob in manager.list_blobs():
            print(blob.name)

        # Download the test file
        manager.download("sync_test.txt", "downloaded_sync_test.txt")

        # Clean up
        manager.delete("sync_test.txt")
        os.remove("sync_test.txt")
        os.remove("downloaded_sync_test.txt")


class TestAsyncAzureBlobStorageManager(unittest.IsolatedAsyncioTestCase):
    # Test the asynchronous class
    async def test_async_class(self):
        async_manager = await AsyncAzureBlobStorageManager.create_instance(
            CONNECTION_STRING, CONTAINER_NAME
        )

        # Create a test file
        with open("async_test.txt", "w") as f:
            f.write("This is a test file")

        # Upload the test file
        await async_manager.upload("async_test.txt", "async_test.txt")

        # List blobs
        blobs = await async_manager.list_blobs()
        for blob in blobs:
            print(blob.name)

        # Download the test file
        await async_manager.download("async_test.txt", "downloaded_async_test.txt")

        # Clean up
        await async_manager.delete("async_test.txt")
        os.remove("async_test.txt")
        os.remove("downloaded_async_test.txt")


if __name__ == "__main__":
    unittest.main()
