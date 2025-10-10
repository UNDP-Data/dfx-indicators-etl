"""
Storage interfaces for I/O operations.
"""

import logging

from pydantic import ValidationError

from ._base import BaseStorage
from .azure import AzureStorage
from .local import LocalStorage

__all__ = ["AzureStorage", "LocalStorage", "get_storage"]

logger = logging.getLogger(__name__)


def get_storage(**kwargs) -> BaseStorage:
    """
    Utility function to get a relevant Storage class based on environment variables.

    The function first attemts to use an AzureStorage before falling back to LocalStorage.

    Parameters
    ----------
    **kwargs
        Keyword arguments passed to the storage class,

    Returns
    -------
    BaseStorage
        Storage class.
    """
    try:
        storage = AzureStorage(**kwargs)
    except ValidationError:
        storage = LocalStorage(**kwargs)
    logger.info("Using %s storage", storage)
    return storage
