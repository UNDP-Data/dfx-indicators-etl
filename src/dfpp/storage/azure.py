"""
Storage interface for I/O operations with Azure Storage.
"""

import os
from dataclasses import dataclass, field

from ._base import BaseStorage

__all__ = ["AzureStorage"]


@dataclass(frozen=True)
class AzureStorage(BaseStorage):
    """
    Storage interface for Azure Blob Storage.
    """

    container_name: str = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    storage_options: dict = field(
        default_factory=lambda: {
            "account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
            "sas_token": os.getenv("AZURE_STORAGE_SAS_TOKEN"),
        }
    )

    def join_path(self, file_path: str) -> str:
        return f"az://{self.container_name}/{file_path}"
