"""
Reusable exception objects.
"""

__all__ = [
    "StorageNotConfigured",
    "AzureStorageNotConfigured",
    "LocalStorageNotConfigured",
]

StorageNotConfigured = ValueError(
    "Env variable for neither Azure Storage or local storage are not set."
)

AzureStorageNotConfigured = ValueError(
    """Env variables for Azure Storage are not set. You must provide `AZURE_STORAGE_ACCOUNT_NAME`,
    `AZURE_STORAGE_CONTAINER_NAME` and `AZURE_STORAGE_SAS_TOKEN`."""
)

LocalStorageNotConfigured = ValueError(
    "Env variable for local storage is not set. You must provide `LOCAL_STORAGE_PATH`"
)
