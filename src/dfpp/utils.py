"""
Utility functions for reading axillary data distributed with the package and performing minor data munging routines.
"""

from importlib import resources
from io import StringIO

import pandas as pd

from . import data

__all__ = ["read_data_text", "read_data_binary", "read_data_csv"]


def read_data_text(file_name: str) -> str:
    """
    Read a text file from the package's `data` directory.

    Parameters
    ----------
    file_name : str
        Name of the file to read.

    Returns
    -------
    str
        Contents of the file as a string.
    """
    with resources.open_text(data, file_name) as file:
        return file.read()


def read_data_binary(file_name: str) -> bytes:
    """
    Read a binary file from the package's `data` directory.

    Parameters
    ----------
    file_name : str
        Name of the file to read.

    Returns
    -------
    bytes
        Contents of the file as bytes.
    """
    with resources.open_binary(data, file_name) as file:
        return file.read()


def read_data_csv(file_name: str, **kwargs) -> pd.DataFrame:
    """
    Read a CSV file from the package's `data` directory.

    Parameters
    ----------
    file_name : str
        Name of the file to read.
    **kwargs
        Additional keywords arguments to pass to `pd.read_csv`.

    Returns
    -------
    pd.DataFrame
        Pandas data frame with the contents of the CSV file.
    """
    content = read_data_text(file_name)
    return pd.read_csv(StringIO(content), **kwargs)
