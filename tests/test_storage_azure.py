"""
Unit tests for Azure module in Store subpackage that implements an interface for Azure Blob Storage.
"""

import json

import pytest

from dfpp.storage.azure import StorageManager


@pytest.mark.asyncio
async def test_client_connection():
    async with StorageManager() as storage_manager:
        # not possible to check with container-level SAS, see
        # https://github.com/Azure/azure-sdk-for-python/issues/18012#issuecomment-1146435699
        # assert await storage_manager.container_client.exists(), "Container does not exist"
        blob_client = storage_manager.container_client.get_blob_client("foo.bar")
        assert isinstance(await blob_client.exists(), bool), "Failed "


@pytest.mark.parametrize(
    "kind",
    ["indicators", "sources"],
)
@pytest.mark.asyncio
async def test_list_configs(kind: str):
    async with StorageManager() as storage_manager:
        configs = await storage_manager.list_configs(kind=kind)
        assert isinstance(configs, list), "Unexpected return type"
        assert len(configs) > 0, "Unexpected number of items"
        assert isinstance(configs[0], str), "Unexpected return item type"


@pytest.mark.parametrize(
    "content",
    ["foo", '{"bar": "baz"}'],
)
@pytest.mark.asyncio
async def test_upload_blob_from_file(content: str, tmp_path):
    path_src = tmp_path / "test.txt"
    path_src.write_text(content)
    async with StorageManager() as storage_manager:
        path = await storage_manager.upload_blob(
            path_or_data_src=str(path_src), path_dst="tests/qux.txt"
        )
        assert isinstance(path, str), "Unexpected return type"
        data = await storage_manager.read_blob(path)
        assert content == data.decode(), "Content mismatch"


@pytest.mark.parametrize(
    "content",
    ["foo", json.dumps({"bar": "baz"})],
)
@pytest.mark.asyncio
async def test_upload_blob_from_data(content: str | dict, tmp_path):
    async with StorageManager() as storage_manager:
        path = await storage_manager.upload_blob(
            path_or_data_src=content.encode(), path_dst="tests/qux.txt"
        )
        assert isinstance(path, str), "Unexpected return type"
        data = await storage_manager.read_blob(path)
        assert content == data.decode(), "Content mismatch"
