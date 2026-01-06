"""
ETL components to process data from the IMF DataMapper API.
See https://www.imf.org/external/datamapper/api/help.
"""

import httpx
import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm

from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the IMF DataMapper API.
    """

    uri: HttpUrl = Field(
        default="https://www.imf.org/external/datamapper/api/v1/",
        frozen=True,
        validate_default=True,
    )

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the the IMF DataMapper API,

        Parameters
        ----------
        **kwargs
            Extra arguments to pass to `_get_data`.

        Returns
        -------
        pd.DataFrame
            Raw data from the API for the indicators with supported disaggregations.
        """
        df_metadata = self.get_metadata()
        data = []
        with self.client as client:
            for _, row in tqdm(df_metadata.iterrows(), total=len(df_metadata)):
                df = self._get_data(row.code, client=client, **kwargs)
                if df is None:
                    continue
                df["indicator_name"] = f"{row['name']}, {row['unit']} [{row['code']}]"
                data.append(df)
        return pd.concat(data, axis=0, ignore_index=True)

    def _get_metadata(self) -> pd.DataFrame:
        """
        Get series metadata from the IMF DataMapper API indicators endpoint.

        Returns
        -------
        pd.DataFrame
            Data frame with three columns: `code`, `name` and `unit`.
        """
        response = self.client.get("indicators")
        response.raise_for_status()
        data = response.json()
        data = [
            {"series_id": series_id} | metadata
            for series_id, metadata in data["indicators"].items()
            if series_id
        ]
        columns = {"series_id": "code", "label": "name", "unit": "unit"}
        df = pd.DataFrame(data).reindex(columns=columns).rename(columns=columns)
        return df

    def _get_data(
        self,
        indicator_code: str,
        client: httpx.Client,
        start_period: int = 1950,
        end_period: int = 2050,
        **kwargs,
    ) -> pd.DataFrame | None:
        """
        Get series data from the IMF DataMapper API endpoint.

        Parameters
        ----------
        indicator_code : str
            Indicator code to retrieve data for. See `_get_metadata`.
            Retrieve data until this date.
        client : httpx.Client,
            Client with a `base_url` to use for making an HTTP GET request.

        Returns
        -------
        pd.DataFrame or None
            Data frame with raw data as returned by the API or None.
        """
        params = {
            "periods": ",".join(map(str, range(start_period, end_period)))
        } | kwargs
        if client.base_url is None:
            raise ValueError("`client` must include a `base_url`.")


        response = client.get(indicator_code, params=params)
        response.raise_for_status()


        data = response.json()
        if (values := data.get("values")) is None:
            return None
        dfs = []
        for country_code, records in values[indicator_code].items():
            df = pd.DataFrame(records.items(), columns=["year", "value"])
            df["country_code"] = country_code
            dfs.append(df)
        return pd.concat(dfs, axis=0, ignore_index=True)


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the IMF DataMapper API.
    """

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Tranform raw data from the IMF DataMapper API.

        No transformation is necessary for this source.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data as returned by the retrieval section.

        Returns
        -------
        pd.DataFrame
            Standardised data frame.
        """
        return df
