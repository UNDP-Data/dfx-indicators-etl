"""
Storage class for a local file system.
"""

from typing import Any

from ..exceptions import LocalStorageNotConfiguredError
from ..settings import SETTINGS
from ._base import BaseStorage

__all__ = ["LocalStorage"]


class LocalStorage(BaseStorage):
    """
    Storage interface for a local file system.
    """

    @property
    def storage_options(self) -> dict[str, Any] | None:
        """
        Storage options to be passed to `to_parquet` in `pandas`.
        """
        return None

    def join_path(self, file_path: str) -> str:
        """
        Get a full path to a file.

        The function creates any intermediary directories if they
        don't already exists.

        Parameters
        ----------
        file_path : str
            Relative path to the file within the `folder_path`.

        Returns
        -------
        str
            Full path to the blob file in the storage account.
        """
        if SETTINGS.local_storage is None:
            raise LocalStorageNotConfiguredError
        file_path = SETTINGS.local_storage.joinpath(file_path)
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
        return str(file_path)
