"""
Storage interfaces for I/O operations.
"""

import logging

from ..settings import SETTINGS
from ._base import BaseStorage
from .azure import AzureStorage
from .local import LocalStorage

__all__ = ["BaseStorage", "AzureStorage", "LocalStorage", "get_storage"]

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
        raise KeyError(
            "Environment variables for neither Azure Storage nor local storage are not set."
        )
    logger.info("Using %s storage", storage)
    return storage
