"""
Package settings based on environment variables.
"""

from typing import Any

from pydantic import BaseModel, DirectoryPath, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["SETTINGS"]


class AzureStorage(BaseModel):
    """
    Storage settings for Azure Storage.
    """

    account_name: str
    container_name: str
    sas_token: str = Field(
        description="""SAS token for a container in the Azure Storage account. See
        https://docs.azure.cn/en-us/ai-services/language-service/native-document-support/shared-access-signatures
        """,
        repr=False,
    )

    @property
    def storage_options(self) -> dict[str, Any] | None:
        """
        Get storage options to be passed to `pandas` IO methods like `to_parquet`.
        """
        return {"account_name": self.account_name, "sas_token": self.sas_token}


class Settings(BaseSettings):
    """
    Package settings, including secrets.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_nested_delimiter="_",
        env_nested_max_split=1,
    )

    db_conn: PostgresDsn | None = Field(default=None, alias="DB_CONNECTION", repr=False)
    http_timeout: int = Field(
        default=30, description="Default client timeout in seconds for HTTP requests."
    )
    year_min: int = Field(
        default=2005,
        description="Minimum year value to be used as a cut-off point for the data. Observations "
        "older than this year will be removed",
    )
    azure_storage: AzureStorage | None = Field(default=None)
    local_storage: DirectoryPath | None = Field(
        default=None, alias="LOCAL_STORAGE_PATH"
    )


SETTINGS = Settings()
