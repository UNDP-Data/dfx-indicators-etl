"""
Base class to build storage interfaces for remote and local file systems.
"""

import os
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, final

import pandas as pd

__all__ = ["BaseStorage"]


class BaseStorage(ABC):
    """
    Abstract class to build storage interfaces.
    """

    @property
    @abstractmethod
    def storage_options(self) -> dict[str, Any] | None:
        """
        Storage options to be passed to `read_parquet` and `to_parquet` in `pandas`.
        """

    @final
    @property
    def version(self) -> str:
        """
        Get a version timestamp for versioning data in the storage.

        Returns
        -------
        str
            Version string in the format vYYYY-MM-DD.
        """
        return datetime.now(UTC).strftime("v%y-%m-%d")

    @abstractmethod
    def join_path(self, file_path: str) -> str:
        """
        Get a full path to a file.
        """

    @final
    def publish_dataset(self, df: pd.DataFrame, folder_path: str = "") -> str:
        """
        Publish a dataset to the storage.

        Parameters
        ----------
        df : pd.DataFrame
            Dataset to be published. The data frame must contain
            a `name` attribute.
        folder_path : str, optional
            Path within the container or bucket to write the file to.

        Returns
        -------
        str
            Full path to the file in the storage.
        """
        if getattr(df, "name") is None:
            raise AttributeError("Data frame name must be provided.")
        file_path = os.path.join(self.version, folder_path, f"{df.name}.parquet")
        file_path = self.join_path(file_path)
        df.to_parquet(file_path, storage_options=self.storage_options, index=False)
        return str(file_path)

    @final
    def read_dataset(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Read a dataset from the storage.

        Parameters
        ----------
        file_path : str
            Full path to the file on the remote storage.
        **kwargs
            Additional keyword arguments to pass to a reading
            function in `pandas`.

        Returns
        -------
        pd.DataFrame
            Dataset data as a data frame.
        """
        _, extension = os.path.splitext(file_path)
        match extension:
            case ".parquet":
                return pd.read_parquet(
                    file_path, storage_options=self.storage_options, **kwargs
                )
            case ".csv":
                return pd.read_csv(
                    file_path,
                    storage_options=self.storage_options,
                    low_memory=False,
                    **kwargs,
                )
            case ".xlsx":
                return pd.read_excel(
                    file_path, storage_options=self.storage_options, **kwargs
                )
            case _:
                raise ValueError(f"`{extension}` extension is not supported.")
