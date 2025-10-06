"""
Storage interfaces for I/O operations.
"""

import os

from ._base import BaseStorage
from .azure import AzureStorage
from .local import LocalStorage

__all__ = ["AzureStorage", "LocalStorage", "get_storage"]


def get_storage(**kwargs) -> BaseStorage:
    """
    Utility function to get appropriate Storage class based on environment variables.

    Returns
    -------
    BaseStorage
        Storage class.
    """
    if os.getenv("AZURE_STORAGE_CONTAINER_NAME") is not None:
        storage = AzureStorage(**kwargs)
    else:
        storage = LocalStorage(**kwargs)
    return storage
