import os
import unittest

from dfpp.storage import AsyncAzureBlobStorageManager, AzureBlobStorageManager

CONNECTION_STRING = os.environ["CONNECTION_STRING"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]
ROOT_FOLDER = os.environ["ROOT_FOLDER"]


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
        dst_test_path = os.path.join("DataFuturePlatform", "pipeline", "sync_test.txt")
        manager.upload(dst_path=dst_test_path, src_path="sync_test.txt")

        # Test list_and_filter
        filtered_blobs = manager.list_and_filter(prefix="sync_test")
        for blob in filtered_blobs:
            print(blob.name)

        # List blobs
        all_listed_blobs = manager.list_blobs(prefix="DataFuturePlatform/pipeline")
        for blob in all_listed_blobs:
            print(blob.name)

        # Test hierarchical_list
        hierarchical_blobs = manager.hierarchical_list(
            prefix="DataFuturePlatform/pipeline"
        )
        for blob in hierarchical_blobs:
            print(blob.name)

        # Test list_indicators
        indicators = manager.list_indicators()
        print(indicators)

        # Test list_sources
        sources = manager.list_sources()
        print(sources)

        # Test get_source_config
        source_config = manager.get_source_config()
        print(source_config)

        # Test get_source_indicator_config
        source_indicator_config = manager.get_source_indicator_config()
        print(source_indicator_config)

        # Test get_utility_file
        utility_config = manager.get_utility_file(utility_file="testing.cfg")
        print(utility_config)

        # Test get_source_files
        for dataset in manager.get_source_files(source_type="raw"):
            print(dataset)

        # Test get_output_files
        for file_name, file_content in manager.get_output_files(
            subfolder="access_all_data"
        ):
            print(file_name, file_content)

        # Download the test file
        manager.download(blob_name=dst_test_path, dst_path="downloaded_sync_test.txt")

        # Clean up
        manager.delete(blob_name=dst_test_path)
        os.remove("sync_test.txt")
        os.remove("downloaded_sync_test.txt")


class TestAsyncAzureBlobStorageManager(unittest.IsolatedAsyncioTestCase):
    async def test_async_class(self):
        async_manager = await AsyncAzureBlobStorageManager.create_instance(
            connection_string=CONNECTION_STRING, container_name=CONTAINER_NAME
        )

        # Create a test file
        with open("async_test.txt", "w") as f:
            f.write("This is a test file")

        # Upload the test file
        dst_test_path = os.path.join("DataFuturePlatform", "pipeline", "async_test.txt")
        await async_manager.upload(dst_path=dst_test_path, src_path="async_test.txt")

        # Test list_and_filter
        filtered_blobs = await async_manager.list_and_filter(prefix="async_test")
        for blob in filtered_blobs:
            print(blob.name)

        # Test list_blobs
        all_listed_blobs = await async_manager.list_blobs(
            prefix="DataFuturePlatform/pipeline"
        )
        for blob in all_listed_blobs:
            print(blob.name)

        # Test _yield_blobs
        async for blob in async_manager._yield_blobs(
            prefix="DataFuturePlatform/pipeline"
        ):
            print(blob.name)

        # Test hierarchical_list
        async for blob in async_manager.hierarchical_list(
            prefix="DataFuturePlatform/pipeline"
        ):
            print(blob.name)

        # Test list_indicators
        indicators = await async_manager.list_indicators()
        print(indicators)

        # Test list_sources
        sources = await async_manager.list_sources()
        print(sources)

        # Test get_source_config
        source_config = await async_manager.get_source_config()
        print(source_config)

        # Test get_source_indicator_config
        source_indicator_config = await async_manager.get_source_indicator_config()
        print(source_indicator_config)

        # Test get_utility_file
        utility_config = await async_manager.get_utility_file(
            utility_file="testing.cfg"
        )
        print(utility_config)

        # Test get_source_files
        async for dataset in async_manager.get_source_files(source_type="raw"):
            print(dataset)

        # Test get_output_files
        async for file_name, file_content in async_manager.get_output_files(
            subfolder="access_all_data"
        ):
            print(file_name, file_content)

        # Download the test file
        await async_manager.download(
            blob_name=dst_test_path, dst_path="downloaded_async_test.txt"
        )

        # Clean up
        await async_manager.delete(blob_name=dst_test_path)
        os.remove("async_test.txt")
        os.remove("downloaded_async_test.txt")


if __name__ == "__main__":
    unittest.main()
