"""
Base classes for building ETL pipelines.

Each new pipeline must implement a source-specific retriever and transformer
classes by inheriting from the base classes defined below.
"""
import os.path
from abc import ABC, abstractmethod
from io import BytesIO
from pathlib import Path
from typing import final
from urllib.parse import urlparse
import tempfile
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
import logging
logger = logging.getLogger(__name__)

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
    http_pattern_char :str = Field(
        default='*',
        description='A character to be used for manual sources located in public UNDP azure containers'
    )
    @final
    @property
    def resolved_uri(self) -> str:
        import xml.etree.ElementTree as ET
        if not self.http_pattern_char in self.uri.path:return self.uri
        container, *parts = self.uri.path.strip('/').split('/')

        expression = parts[-1]
        prefix, *rest = expression.split(self.http_pattern_char)

        url = f'{self.uri.scheme}://{self.uri.host}/{container}?restype=container&comp=list&prefix={prefix}'
        response = self.client.get(url=url)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        correct_url = root.find(".//{*}Url")
        if correct_url is None:
            raise Exception(f'Could not retrieve the correct url for {self.uri} from {url}')
        blob_url = correct_url.text
        return HttpUrl(url=blob_url)


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
        # Build a proper Timeout object
        timeout_config = httpx.Timeout(
            connect=SETTINGS.pipeline.http_timeout_connect,
            read=SETTINGS.pipeline.http_timeout_read,
            pool=SETTINGS.pipeline.http_timeout_pool,
            write=10.0  # Standard write timeout
        )
        return httpx.Client(
            base_url=str(uri),
            headers=self.headers,
            timeout=timeout_config,  # Use the object here
            follow_redirects=True,
            # Performance tip: Increase limits for your 1,167 requests
            #limits=httpx.Limits(max_connections=100, max_keepalive_connections=50)
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
        chunk_size:int|None = None,
        use_cache = False,
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
            Parameters to include the GET request.
        client: httpx.Client, optional
            Client to use to make a request.
        chunk_size: int, the numbert opf bytes to request
        use_cache: bool, if True stram the bytes into a temporary file, otherwise keep the bytes in RAM
        **kwargs
            Extra arguments to be passed to `pd.read_csv`.

        Returns
        -------
        pd.DataFrame or None
            Pandas data frame if the request has succeeded or None if it has raised an error.
        """


        # 1. Initialize at the top to prevent UnboundLocalError in 'finally'
        should_close = False
        client_to_use = client

        if client_to_use is None:
            client_to_use = self.client
            should_close = True

        try:

            if not use_cache:
                with client_to_use.stream("GET", url, params=params) as response:
                    response.raise_for_status()

                    # 2. Collect chunks into a memory buffer
                    with BytesIO() as buffer:
                        for chunk in response.iter_bytes(chunk_size=chunk_size) :
                            if chunk:
                                buffer.write(chunk)

                        # 3. Reset buffer position for Pandas
                        buffer.seek(0)

                        # 4. Load into DataFrame
                        if buffer.getbuffer().nbytes == 0:
                            raise pd.errors.EmptyDataError()
                        return pd.read_csv(buffer, low_memory=False)

            else:
                with tempfile.NamedTemporaryFile(dir='/tmp', suffix=".csv") as tmp:
                    try:
                        with client_to_use.stream("GET", url, params=params) as response:
                            response.raise_for_status()
                            for chunk in response.iter_bytes(chunk_size=chunk_size):
                                if chunk:
                                    tmp.write(chunk)

                        tmp.flush()

                        # 3. Check if we actually got data before giving it to PyArrow
                        if Path(tmp.name).stat().st_size == 0:
                            raise pd.errors.EmptyDataError()

                        return pd.read_csv(
                            tmp.name,
                            engine="pyarrow",
                            dtype_backend="pyarrow"
                        )

                    except Exception as e:
                        logger.error(f"ETL Stream Error: {e}")
                        raise e
        finally:
            # 4. Safely close only if we are the ones who opened it
            if should_close and client_to_use:
                client_to_use.close()


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
