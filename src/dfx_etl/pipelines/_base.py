"""
Base classes for building ETL pipelines.

Each new pipeline must implement a source-specific retriever and transformer
classes by inheriting from the base classes defined below.
"""

from abc import ABC, abstractmethod
from io import BytesIO
from typing import final

import httpx
import pandas as pd
from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    HttpUrl,
    ValidationError,
)

__all__ = ["BaseRetriever", "BaseTransformer"]


class BaseRetriever(BaseModel, ABC):
    """
    Abstract class to build retrievers for data sources.

    See Also
    --------
    BaseTransformer : Another abstract class used to define the transform step of the ETL pipeline.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    uri: AnyUrl | FilePath = Field(
        ...,
        frozen=True,
        description="URL or file path to the source.",
        examples=["https://ghoapi.azureedge.net/api/"],
    )
    headers: dict | None = Field(
        default=None,
        description="Headers to be used by `httpx.Client` for HTTP requests",
        examples=[
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                    (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.3"
            }
        ],
    )

    @property
    def client(self) -> httpx.Client:
        """
        An HTTP client for making requests.

        Returns
        -------
        httpx.Client
            HTTP client with `base_url` and `headers` from the instance properties.
        """
        try:
            uri = HttpUrl(self.uri)
        except ValidationError:
            raise TypeError(
                "`client` is only applicable when `uri` is an HTTP location"
            )
        return httpx.Client(base_url=str(uri), headers=self.headers, timeout=30)

    @abstractmethod
    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve indicator data from a source.

        This function must be overwritten by a child class. It can implement arbitrary
        logic necessary to retrieve data from the source and may return a data frame
        in any format. The returned object is expected to be processed by a `BaseTransformer`
        class.
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

    See Also
    --------
    BaseRetriever : Another abstract class used to define the retrieve step of the ETL pipeline.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    @abstractmethod
    def __call__(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data.
        """
