"""
Storage class for a local file system.
"""

from typing import Any

from pydantic import DirectoryPath, Field
from pydantic_settings import BaseSettings

from ._base import BaseStorage

__all__ = ["LocalStorage"]


class Settings(BaseSettings):
    """
    Storage settings for local storage.
    """

    folder_path: DirectoryPath = Field(
        validation_alias="LOCAL_DATA_PATH",
        description="Directory path to be used for writing data",
    )


class LocalStorage(BaseStorage, Settings):
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
        file_path = self.folder_path.joinpath(file_path)
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
        return str(file_path)
