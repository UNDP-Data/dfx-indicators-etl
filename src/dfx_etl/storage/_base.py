"""
Base class to build storage interfaces for remote and local file systems.
"""

import os
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, final
import logging
import pandas as pd

logger = logging.getLogger(__name__)
__all__ = ["BaseStorage"]
FORMATS = 'csv', 'parquet'

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
    def write_dataset(self, df: pd.DataFrame, folder_path: str = "", format='parquet') -> str:
        """
        Write a dataset to the storage.

        Parameters
        ----------
        df : pd.DataFrame
            Dataset to be written. The data frame must contain
            a `name` attribute.
        folder_path : str, optional
            Path within the container or bucket to write the file to.
        format: str, optional
            The serialization format

        Returns
        -------
        str
            Full path to the file in the storage.
        """
        if getattr(df, "name") is None:
            raise AttributeError("Data frame name must be provided.")
        file_name = f"{df.name}.{format}"
        file_path = os.path.join(self.version, folder_path, file_name)
        file_path = self.join_path(file_path)
        method_name = f'to_{format}'
        serialization_method = getattr(df, method_name)
        serialization_method(file_path, storage_options=self.storage_options, index=False)
        logger.info(f'{file_name} was saved to {file_path} ')
        return str(file_path)

    @final
    def read_dataset(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Read a dataset from the storage.

        Parameters
        ----------
        file_path : str
            Relative path to the file in the storage. It may also be a path to a folder containing
            .parquet files to be read and concatenated.
        **kwargs
            Additional keyword arguments to pass to a reading
            function in `pandas`.

        Returns
        -------
        pd.DataFrame
            Dataset data as a data frame.
        """
        file_path = self.join_path(file_path)
        _, extension = os.path.splitext(file_path)
        match extension:
            case ".parquet" | "":
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
