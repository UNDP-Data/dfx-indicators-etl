"""
Base class to build storage interfaces.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, final

import pandas as pd

__all__ = ["BaseStorage"]


@dataclass(frozen=True)
class BaseStorage(ABC):
    """
    Abstract class to build storage interfaces.
    """

    container_name: str = field(
        metadata={"description": "Container or bucket name of the remote storage."},
    )
    storage_options: dict[str, Any] = field(
        metadata={
            "description": "Storage options to be passed to `to_parquet` in `pandas`"
        },
    )
    version: str = field(
        default_factory=lambda: datetime.now(UTC).strftime("v%y-%m-%d"),
        metadata={
            "description": "String name to be used as a folder name for versioning runs."
        },
    )

    @abstractmethod
    def join_path(self, file_path: str) -> str:
        """
        Get a full path a file on the remote.
        """

    @final
    def publish_dataset(self, df: pd.DataFrame, folder_path: str = "") -> str:
        """
        Publish a dataset to the remote storage.

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
            Full path to the file on the remote storage.
        """
        if getattr(df, "name") is None:
            raise AttributeError("Data frame name must be provided.")
        file_path = os.path.join(self.version, folder_path, f"{df.name}.parquet")
        file_path = self.join_path(file_path)
        df.to_parquet(file_path, storage_options=self.storage_options)
        return file_path

    @final
    def read_dataset(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Read a dataset from the remote storage.

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
                    file_path, storage_options=self.storage_options, **kwargs
                )
            case ".xlsx":
                return pd.read_excel(
                    file_path, storage_options=self.storage_options, **kwargs
                )
            case _:
                raise ValueError(f"`{extension}` extension is not supported.")
