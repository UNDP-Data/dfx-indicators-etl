"""
Storage class for a local file system.
"""

from typing import Any

from ..settings import SETTINGS
from ._base import BaseStorage

__all__ = ["LocalStorage"]


class LocalStorage(BaseStorage):
    """
    Storage interface for a local file system.
    """

    def __init__(self):
        """
        Perform validation during initialisation.
        """
        if SETTINGS.local_storage is None:
            raise KeyError(
                "Environment variable for local storage is not set. You must provide "
                "`LOCAL_STORAGE_PATH`"
            )

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
        file_path = SETTINGS.local_storage.joinpath(file_path)
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
        return str(file_path)
