"""
Storage interfaces for I/O operations.
"""

from .azure import AzureStorage
from .local import LocalStorage

__all__ = ["AzureStorage", "LocalStorage"]
