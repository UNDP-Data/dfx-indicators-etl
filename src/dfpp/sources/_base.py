"""
Base classes for building ETL pipelines.
"""

from abc import ABC, abstractmethod
from io import BytesIO
from typing import final

import httpx
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, FilePath, HttpUrl

__all__ = ["BaseRetriever", "BaseTransformer"]


class BaseRetriever(BaseModel, ABC):
    """
    Abstract class to build retrievers for data sources.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    uri: HttpUrl | FilePath = Field(
        ...,
        frozen=True,
        description="URL or file path to the source.",
    )
    headers: dict = Field(
        default_factory=lambda: {"User-Agent": "python"},
        description="Headers to be used by `httpx.Client` for HTTP requests",
    )

    @property
    def client(self) -> httpx.Client:
        if not isinstance(self.uri, HttpUrl):
            raise TypeError(
                "`client` is only applicable when `uri` is an HTTP location"
            )
        return httpx.Client(base_url=str(self.uri), headers=self.headers, timeout=30)

    @abstractmethod
    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve indicator data.
        """

    @final
    def read_csv(
        self,
        url: str,
        params: dict | None = None,
        client: httpx.Client | None = None,
        **kwargs
    ) -> pd.DataFrame | None:
        """
        Read a CSV file from a remote location using an HTTP GET request.

        This method may be more efficient than using `pd.read_csv` directly when a custom client
        is provided and when the method is repeatedly invoked in a loop.

        Parameters
        ----------
        url : str
            URL to read a CSV from. This may be a relative URL if a client with
            `base_url` is provided.
        params : dict, optional
            Parameters to include an the GET request.
        client: httpx.Client, optional
            Client to use to make a request.
        **kwargs
            Extra arguments to be passed to `pd.read_csv`.

        Returns
        -------
        pd.DataFrame or None
            Pandas data frame if the request has succeeded or None if it has raised an error.
        """
        try:
            if client is None:
                response = httpx.get(url, params=params)
            else:
                response = client.get(url, params=params)
            response.raise_for_status()
        except httpx.ReadTimeout as error:
            print(error)
            return None
        except httpx.HTTPStatusError as error:
            print(error)
            return None
        return pd.read_csv(BytesIO(response.content), low_memory=False, **kwargs)


class BaseTransformer(BaseModel, ABC):
    """
    Abstract class to build transformers for data sources.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    @abstractmethod
    def __call__(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data.
        """
