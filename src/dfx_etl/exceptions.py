"""
Reusable exception objects.
"""

__all__ = ["StorageRequiredError"]


class StorageRequiredError(ValueError):
    """
    Custom error for when storage is not passed to the retriever in the manual source.
    """

    def __init__(self):
        super().__init__("The storage interface must be provided to this retriever.")
