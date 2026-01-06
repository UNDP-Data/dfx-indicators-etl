"""
ETL components to process data from the UN Stats SDG API.
See https://unstats.un.org/sdgs/UNSDGAPIV5/swagger/index.html.
"""

import httpx
import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm

from ..utils import replace_country_metadata, to_snake_case
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the UN Stats SDG API.
    """

    uri: HttpUrl = Field(
        default="https://unstats.un.org/sdgapi/v1/sdg/",
        frozen=True,
        validate_default=True,
    )

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the UN Stats SDG API.

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
                data.append(df)
        df_data = pd.concat(data, axis=0, ignore_index=True)
        return df_data

    def _get_metadata(self) -> pd.DataFrame:
        """
        Get series metadata from the UN Stats SDG API.

        Returns
        -------
        pd.DataFrame
            Data with series metadata.
        """
        response = self.client.get("series/list", timeout=60)
        response.raise_for_status()
        columns = {"code": "code", "description": "name"}
        df = pd.DataFrame(response.json())
        return df.reindex(columns=columns).rename(columns=columns)

    def _get_data(
        self,
        indicator_code: str,
        client: httpx.Client | None = None,
        **kwargs,
    ) -> pd.DataFrame | None:
        """
        Get series data from the UN Stats SDG API.

        Parameters
        ----------
        indicator_code : str
            Indicator code. See `_get_metadata`.

        Returns
        -------
        pd.DataFrame or None
            Data frame with country data in the wide format.

        """
        pages, df = self._get_page(indicator_code, 1, client, **kwargs)
        data = [df]
        for page in range(2, pages + 1):
            _, df = self._get_page(indicator_code, page, client, **kwargs)
            data.append(df)
        return pd.concat(data, axis=0, ignore_index=True)

    def _get_page(
        self,
        indicator_code: str,
        page: int,
        client: httpx.Client | None = None,
        **kwargs,
    ) -> pd.DataFrame | None:
        """
        Get series data from the UN Stats SDG API.

        Parameters
        ----------
        indicator_code : str
            Indicator code. See `_get_metadata`.

        Returns
        -------
        pd.DataFrame or None
            Data frame with country data in the wide format.

        """
        data = []
        params = {
            "seriesCode": indicator_code,
            "pageSize": 1_000,
            "page": page,
        } | kwargs
        response = client.get("Series/Data", params=params)
        response.raise_for_status()
        data = response.json()
        pages = data["totalPages"]
        df = pd.DataFrame(data["data"])
        return pages, df


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the UN Stats SDG API.
    """

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data from UNICEF SDMX API.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data frame.

        Returns
        -------
        pd.DataFrame
            Transformed data frame in the canonical format.
        """
        columns = {
            "indicator_name": "indicator_name",
            "alpha_3_code": "country_code",
            "timePeriodStart": "year",
            "value": "value",
            "prop_nature": "observation_type",
        }
        df["alpha_3_code"] = replace_country_metadata(
            df["geoAreaCode"], "m49", "iso-alpha-3"
        )
        df["value"] = df["value"].replace({"NaN": None})
        df.dropna(subset=["alpha_3_code", "value"], ignore_index=True, inplace=True)
        for column, prefix in (("attributes", "prop"), ("dimensions", "disagr")):
            df = df.join(
                pd.DataFrame(df[column].tolist())
                .rename(lambda name: to_snake_case(name, prefix=prefix), axis=1)
                .fillna("Total")  # Fill as 'Total' when no dimension exist
            )
        df["indicator_name"] = df.apply(
            lambda row: f"{row['seriesDescription']}, {row['prop_units']} [{row['series']}]",
            axis=1,
        )
        return df.rename(columns=columns)
