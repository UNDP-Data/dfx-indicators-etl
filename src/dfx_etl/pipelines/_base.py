"""
Base classes for building ETL pipelines.

Each new pipeline must implement a source-specific retriever and transformer
classes by inheriting from the base classes defined below.
"""

from abc import ABC, abstractmethod
from io import BytesIO
from pathlib import Path
from typing import final
from urllib.parse import urlparse

import httpx
import pandas as pd
import pandera as pa
from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    HttpUrl,
    ValidationError,
)

from ..settings import SETTINGS
from ..utils import get_country_metadata
from ..validation import DataSchema, MetadataSchema

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

    @final
    @property
    def provider(self) -> str:
        """
        Get a tandardised provider name based on the pipeline module name.

        The provider name is also used as a file name when saving data.
        """

        return self.__module__.split(".")[-1]

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
        return httpx.Client(
            base_url=str(uri),
            headers=self.headers,
            timeout=SETTINGS.pipeline.http_timeout,
        )

    @abstractmethod
    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve indicator data from a source.

        This function must be overwritten by a child class. It can implement arbitrary
        logic necessary to retrieve data from the source and may return a data frame
        in any format. The returned object is expected to be processed by a `BaseTransformer`
        class.
        """

    def _get_metadata(self) -> pd.DataFrame:
        """
        Optional method to get indicator metadata from the source.

        Returns
        -------
        pd.DataFrame
            Indicator metadata data frame.
        """
        raise NotImplementedError(
            "Subclasses should override `_get_metadata` if applicable."
        )

    @final
    @pa.check_output(MetadataSchema)
    def get_metadata(self) -> pd.DataFrame:
        """
        Get indicator metadata from the source if applicable.

        Returns
        -------
        pd.DataFrame
            Indicator metadata as per the schema.
        """
        return self._get_metadata()

    @final
    def read_csv(
        self,
        url: str,
        params: dict | None = None,
        client: httpx.Client | None = None,
        **kwargs,
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

    @final
    @pa.check_output(DataSchema)
    def __call__(self, df: pd.DataFrame, provider: str, **kwargs) -> pd.DataFrame:
        """
        Transform and validate raw data.

        This function also ensures that only the rows with an M49 ISO code
        are kept.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data frame returned by a retriever.
        provider : str
            Value to assign to `provider` column.
        **kwargs
            Keyword arguments passed to `self.transform`.

        Returns
        -------
        pd.DataFrame
            Standardised data frame in line with `DataSchema`.
        """
        df = self.transform(df, **kwargs)
        # Add the data provider if it does not exist yet
        df["provider"] = provider
        # Ensure only areas from UN M49 are present
        country_codes = get_country_metadata("iso-alpha-3")
        df = df.loc[df["country_code"].isin(country_codes)].copy()
        return df

    @abstractmethod
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data.
        """
