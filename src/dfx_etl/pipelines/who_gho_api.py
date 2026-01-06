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

from ..utils import _resolve_dimensions, to_snake_case
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]


warnings.warn(
    """This module is deprecated as the current GHO OData API is set to be removed
    near the end of 2025. See https://www.who.int/data/gho/legacy""",
    category=DeprecationWarning,
    stacklevel=2,
)


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the WHO GHO API.
    """

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
        df_metadata = self.get_metadata()
        data = []
        with self.client as client:
            for _, row in tqdm(df_metadata.iterrows(), total=len(df_metadata)):
                df = self._get_data(row.code, client=client, **kwargs)
                if df is None:
                    continue
                df["indicator_name"] = f"{row['name']} [{row['code']}]"
                data.append(df)
        return pd.concat(data, axis=0, ignore_index=True)

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
        columns = {"IndicatorCode": "code", "IndicatorName": "name"}
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
    """
    A class for transforming raw data from the WHO GHO API.
    """

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data from GHO OData API.

        Note that the source data contains duplicates which are dropped. There are rows that
        do not differ in any column except for value and ID columns. 'DataSourceDim' column
        is treated is a 'source' column and dimension too, because it is used to uniqely identify
        a row in this source too.

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
            "SpatialDim": "country_code",
            "TimeDim": "year",
            "dimension": "dimension",
            "DataSourceDim": "source",
            "NumericValue": "value",
        }

        # Handle dimensions stored in the long format but avoid adding new columns for each
        dims = df.filter(regex=r"^Dim\d$").columns
        df["DataSourceDim"] = df["DataSourceDim"].str.replace("DATASOURCE_", "")
        df["dimension"] = (
            df.apply(
                lambda row: (
                    {
                        to_snake_case(category): row[dim].replace(f"{category}_", "")
                        for dim in dims
                        if (category := row[f"{dim}Type"]) is not None
                    }
                    # Add source as a dimensions to avoid duplicates
                    | {"source": row["DataSourceDim"]}
                )
                or None,
                axis=1,
            )
            .map(lambda x: _resolve_dimensions(x, prefix=""), na_action="ignore")
            .fillna("Total")
        )
        df = df.reindex(columns=columns).rename(columns=columns).reset_index(drop=True)
        # Drop duplicates deterministically
        columns = set(df.columns) - {"value"}
        df.sort_values(list(columns), ignore_index=True, inplace=True)
        df.drop_duplicates(
            subset=list(columns - {"source"}),
            keep="first",
            ignore_index=True,
            inplace=True,
        )
        return df
