"""
Storage interface for I/O operations with Azure Storage.
"""

import re
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings

from ._base import BaseStorage

__all__ = ["AzureStorage"]


class Settings(BaseSettings):
    """
    Storage settings for Azure Blob Storage.
    """

    account_name: str = Field(validation_alias="AZURE_STORAGE_ACCOUNT_NAME")
    container_name: str = Field(validation_alias="AZURE_STORAGE_CONTAINER_NAME")
    sas_token: str = Field(
        validation_alias="AZURE_STORAGE_SAS_TOKEN",
        description="SAS token for a container in the Azure Storage account. See https://docs.azure.cn/en-us/ai-services/language-service/native-document-support/shared-access-signatures",
    )


class AzureStorage(BaseStorage, Settings):
    """
    Storage interface for Azure Blob Storage.
    """

    def __repr__(self):
        return re.sub(r"sas_token='\S+'", "sas_token='REDACTED'", super().__repr__())

    @property
    def storage_options(self) -> dict[str, Any] | None:
        """
        Storage options to be passed to `to_parquet` in `pandas`.
        """
        return {"account_name": self.account_name, "sas_token": self.sas_token}

    def join_path(self, file_path: str) -> str:
        """
        Get an fsspec-compatible path to an object.

        Parameters
        ----------
        file_path : str
            Relative path to the object in the storage container.

        Returns
        -------
        str
            ffstec-compatible full path to the file in the storage container.
        """
        return f"az://{self.container_name}/{file_path}"
