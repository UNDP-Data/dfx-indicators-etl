"""
Storage interface for I/O operations with Azure Storage.
"""

from typing import Any

from ..exceptions import AzureStorageNotConfiguredError
from ..settings import SETTINGS
from ._base import BaseStorage

__all__ = ["AzureStorage"]


class AzureStorage(BaseStorage):
    """
    Storage interface for Azure Blob Storage.
    """

    @property
    def storage_options(self) -> dict[str, Any] | None:
        """
        Storage options to be passed to `to_parquet` in `pandas`.
        """
        if SETTINGS.azure_storage is None:
            raise AzureStorageNotConfiguredError
        return SETTINGS.azure_storage.storage_options

    def join_path(self, file_path: str) -> str:
        """
        Get an fsspec-compatible path to an object.

        Parameters
        ----------
        file_path : str
            Relative path to the object in the storage container.

        Returns
        -------
        str
            ffstec-compatible full path to the file in the storage container.
        """
        if SETTINGS.azure_storage is None:
            raise AzureStorageNotConfiguredError
        return f"az://{SETTINGS.azure_storage.container_name}/{file_path}"
