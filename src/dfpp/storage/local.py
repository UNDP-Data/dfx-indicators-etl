"""
Storage class for a local file system.
"""

from dataclasses import dataclass
from pathlib import Path

from ._base import BaseStorage

__all__ = ["LocalStorage"]


@dataclass(frozen=True)
class LocalStorage(BaseStorage):
    """
    Storage interface for a local file system.

    In this class, `container_name` refers to a folder
    on a local file system where the files will be stored.
    """

    container_name: str = "data"

    def join_path(self, file_path: str) -> str:
        """
        Get a full path to a file.

        The function creates any intermediary directories if they
        don't already exists.

        Parameters
        ----------
        file_path : str
            Partial path to a file, including a file name and arbitrary
            parent folders.

        Returns
        -------
        str
            Relative full path to the file that includes the container name.
        """
        file_path = Path(self.container_name, file_path)
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
        return str(file_path)
