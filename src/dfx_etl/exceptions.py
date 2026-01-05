"""
Reusable exception objects.
"""

__all__ = [
    "StorageNotConfiguredError",
    "AzureStorageNotConfiguredError",
    "LocalStorageNotConfiguredError",
    "StorageRequiredError",
]


class StorageNotConfiguredError(ValueError):
    """
    Custom error for when storage-related environment variables are not set.
    """

    def __init__(self):
        super().__init__(
            "Environment variables for neither Azure Storage nor local storage are not set."
        )


class AzureStorageNotConfiguredError(ValueError):
    """
    Custom error for when Azure storage-related environment variables are not set.
    """

    def __init__(self):
        super().__init__(
            "Environment variables for Azure Storage are not set. You must provide "
            "`AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_CONTAINER_NAME` and "
            "`AZURE_STORAGE_SAS_TOKEN`."
        )


class LocalStorageNotConfiguredError(ValueError):
    """
    Custom error for when Local storage-related environment variables are not set.
    """

    def __init__(self):
        super().__init__(
            "Environment variable for local storage is not set. You must provide "
            "`LOCAL_STORAGE_PATH`"
        )


class StorageRequiredError(ValueError):
    """
    Custom error for when storage is not passed to the retriever in the manual source.
    """

    def __init__(self):
        super().__init__("The storage interface must be provided to this retriever.")
