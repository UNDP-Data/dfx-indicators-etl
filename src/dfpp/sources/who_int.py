"""
ETL components to process data from the WHO GHO API
by the World Health Organisation (WHO)
See https://www.who.int/data/gho/info/gho-odata-api.
"""

import warnings

import httpx
import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm

from ._base import BaseRetriever, BaseTransformer
from ..utils import to_snake_case

__all__ = ["Retriever", "Transformer"]


warnings.warn(
    """This module is deprecated as the current GHO OData API is set to be removed
    near the end of 2025. See https://www.who.int/data/gho/legacy""",
    category=DeprecationWarning,
    stacklevel=2,
)


class Retriever(BaseRetriever):
    uri: HttpUrl = Field(
        default="https://ghoapi.azureedge.net/api/",
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
        with self.client as client:
            for indicator_code in tqdm(indicator_codes):
                df = self._get_data(indicator_code, client=client, **kwargs)
                if df is None:
                    continue
                data.append(df)
        df_data = pd.concat(data, axis=0, ignore_index=True)
        df_data = df_data.merge(
            right=df_metadata,
            how="left",
            left_on="IndicatorCode",
            right_on="indicator_code",
        )
        return df_data

    def _get_dimensions(self) -> dict:
        """
        Get series dimensions from the GHO OData API.

        Returns
        -------
        dict
            Dimensions dictionary.
        """
        response = self.client.get("DIMENSION")
        response.raise_for_status()
        return response.json()["value"]

    def _get_metadata(self) -> pd.DataFrame:
        """
        Get series metadata from the GHO OData API.

        Returns
        -------
        pd.DataFrame
            Data with series metadata.
        """
        response = self.client.get("Indicator")
        response.raise_for_status()
        df = pd.DataFrame(response.json()["value"])
        columns = {"IndicatorCode": "indicator_code", "IndicatorName": "indicator_name"}
        return df.reindex(columns=columns).rename(columns=columns)

    def _get_data(
        self,
        indicator_code: str,
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
        filters = ["NumericValue ne null"]
        for k, v in kwargs.items():
            if isinstance(v, (str, int)):
                filters.append(f"{k} eq '{v}'")
            elif isinstance(v, list):
                filters.append(f"{k} in {tuple(v)}")
            else:
                raise ValueError(
                    f"{k} must be one of (str, int, list). Found {type(v)}"
                )
        filters = f"?$filter={' and '.join(filters)}" if filters else ""
        response = client.get(f"{indicator_code}{filters}")
        response.raise_for_status()
        return pd.DataFrame(response.json()["value"])


class Transformer(BaseTransformer):

    def __call__(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data from GHO OData API.

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
            "IndicatorCode": "indicator_code",
            "indicator_name": "indicator_name",
            "SpatialDim": "country_code",
            "TimeDim": "year",
            "NumericValue": "value",
            "DataSourceDim": "source",  # keep the original source as to avoid duplicates
        }

        # "unstack" dimensions from the long format for Dim1, Dim2 etc columns
        for column in df.filter(regex=r"Dim\dType"):
            dimensions = sorted(df[column].dropna().unique())
            for dimension in dimensions:
                column_dim = f"disagr_{dimension}"
                if column_dim not in df.columns:
                    df[column_dim] = None
                mask = df[column].eq(dimension)
                df.loc[mask, column_dim] = df.loc[mask, column_dim].combine_first(
                    df.loc[mask, column.replace("Type", "")]
                )
        df = (
            df.reindex(columns=columns)
            .rename(columns=columns)
            .join(df.filter(like="disagr_", axis=1).rename(to_snake_case, axis=1))
        )
        df["source"] = df["source"].apply(lambda x: "https://who.int" + f" | {x}")
        return df.reset_index(drop=True)
