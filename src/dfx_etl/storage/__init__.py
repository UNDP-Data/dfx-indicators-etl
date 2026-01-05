"""
Storage interfaces for I/O operations.
"""

import logging

from ..exceptions import StorageNotConfiguredError
from ..settings import SETTINGS
from ._base import BaseStorage
from .azure import AzureStorage
from .local import LocalStorage

__all__ = ["AzureStorage", "LocalStorage", "get_storage"]

logger = logging.getLogger(__name__)


def get_storage(**kwargs) -> BaseStorage:
    """
    Utility function to get a relevant Storage class based on environment variables.

    If configured, the local storage takes precedence over any remote storage.

    Parameters
    ----------
    **kwargs
        Keyword arguments passed to the storage class,

    Returns
    -------
    BaseStorage
        Storage class.
    """
    if SETTINGS.local_storage is not None:
        storage = LocalStorage(**kwargs)
    elif SETTINGS.azure_storage is not None:
        storage = AzureStorage(**kwargs)
    else:
        raise StorageNotConfiguredError
    logger.info("Using %s storage", storage)
    return storage
