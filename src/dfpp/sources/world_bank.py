"""
ETL components to process data from the World Bank Indicator API
by  the World Bank.
See https://datahelpdesk.worldbank.org/knowledgebase/topics/125589-developer-information.
"""

import country_converter as coco
import httpx
import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm

from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]


class Retriever(BaseRetriever):
    uri: HttpUrl = Field(
        default="https://api.worldbank.org/v2/",
        frozen=True,
        validate_default=True,
    )

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the GHO OData API,

        Parameters
        ----------
        **kwargs
            Extra arguments to pass to `_get_data`.

        Returns
        -------
        pd.DataFrame
            Raw data from the API for the indicators with supported disaggregations.
        """
        df_metadata = self._get_metadata()
        indicator_codes = df_metadata["indicator_code"].sample(10).tolist()
        data = []
        with httpx.Client(timeout=30) as client:
            for indicator_code in tqdm(indicator_codes):
                metadata, records = self._get_data(client, indicator_code, page=1)
                data.extend(records)
                if metadata is not None:
                    for page in tqdm(range(2, metadata["pages"])):
                        _, records = self._get_data(client, indicator_code, page=page)
                        data.extend(records)
        return pd.DataFrame(data)

    def _get_metadata(self) -> pd.DataFrame:
        """
        Get a single metadata page.
        """
        data = []
        params = {"format": "json", "per_page": 100, "page": 1}
        with self.Client() as client:
            metadata, indicators = client.get("indicator", params=params)
            data.extend(indicators)
            for page in tqdm(range(2, metadata["pages"])):
                params["page"] = page
                metadata, indicators = client.get("indicator", params=params)
                data.extend(indicators)
        return pd.DataFrame(data)

    def _get_data(
        self,
        indicator_code: str,
        page: int,
        client: httpx.Client | None = None,
        **kwargs,
    ) -> pd.DataFrame | None:
        """
        Get series data from the GHO OData API.

        Parameters
        ----------
        indicator_code : str
            Indicator code. See `_get_metadata`.

        Returns
        -------
        pd.DataFrame or None
            Data frame with country data in the wide format.

        """
        response = client.get(
            f"country/all/indicator/{indicator_code}",
            params={
                "date": "2015:2025",
                "page": page,
                "per_page": 1_000,
                "format": "json",
            },
        )
        response.raise_for_status()
        metadata, data = None, None
        if len(data := response.json()) == 1:
            metadata = data[0]
            if "message" in metadata:
                print(metadata)
            metadata = None
        elif len(data) == 2:
            metadata, data = data
        return metadata, data


class Transformer(BaseTransformer):

    def __call__(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform data from the World Bank Indicator API.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data frame.

        Returns
        -------
        pd.DataFrame
            Transformed data frame in the canonical format.
        """

        df = df.copy()
        for column in ("indicator", "country"):
            df = df.join(
                pd.DataFrame(df[column].tolist()).rename(
                    lambda x: f"{column}_{x}", axis=1
                )
            )
            df.drop(column, axis=1, inplace=True)
        df.replace({"": None}, inplace=True)
        cc = coco.CountryConverter()
        df["country_value"] = cc.pandas_convert(
            df["country_value"], to="ISO3", not_found=None
        )

        for column in ("country_id", "country_value"):
            df["countryiso3code"] = df["countryiso3code"].combine_first(df[column])
        df.dropna(subset=["countryiso3code"], inplace=True)

        # keep only yearly data
        df = df.loc[df["date"].str.isdigit()].copy()

        columns = {
            "indicator_id": "indicator_code",
            "indicator_value": "indicator_name",
            "countryiso3code": "country_code",
            "date": "year",
            "value": "value",
        }
        return df.reindex(columns=columns).rename(columns=columns)
