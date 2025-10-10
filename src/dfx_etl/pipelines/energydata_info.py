"""
ETL routines for data from ENERGYDATA.INFO.
See https://energydata.info.
"""

import country_converter as coco
import pandas as pd
from pydantic import Field, HttpUrl

from ..validation import PREFIX_DISAGGREGATION
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]


class Retriever(BaseRetriever):
    """
    A class for retrieving data from ENERGYDATA.INFO.
    """

    uri: HttpUrl = Field(
        default="https://energydata.info/dataset/b33e5af4-bd51-4ee0-a062-29438471db27/resource/6938ec3a-f7bb-4493-86ba-f28faa62f139/download/eleccap_20220404-201215.xlsx",
        frozen=True,
        validate_default=True,
    )

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the energydata.info.

        Parameters
        ----------
        **kwargs
            Extra arguments to pass to `pd.read_excel`.

        Returns
        -------
        pd.DataFrame
            Raw data from the API for the indicators with supported disaggregations.
        """
        return pd.read_excel(str(self.uri), header=1, na_values=[".."], **kwargs)


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the ENERGYDATA.INFO.
    """

    def __call__(self, df: pd.DataFrame, **kwargs):
        """
        Transform ELECCAP series from energydata.info.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data as returned by the retriever.

        Returns
        -------
        pd.DataFrame
            Standardised data frame.
        """
        cc = coco.CountryConverter()
        df = df.copy()
        df.columns = [
            "country",
            PREFIX_DISAGGREGATION + "energy_technology",
            PREFIX_DISAGGREGATION + "grid_connection",
            "year",
            "value",
        ]
        df.ffill(inplace=True)
        df["country_code"] = cc.pandas_convert(df["country"], to="ISO3")
        df.drop(columns=["country"], inplace=True)
        df = df[df["country_code"] != "not found"].reset_index(drop=True)
        df["unit"] = "Megawatt"
        df["source"] = "https://energydata.info/"
        df["indicator_code"] = "irena_eleccap"
        df["indicator_name"] = (
            "Installed electricity capacity by country/area (MW) by Country/area, Technology, Grid connection and Year"
        )
        # remove rows without values
        df.dropna(subset=["value"], inplace=True)
        # only remove full duplicates
        df.drop_duplicates(ignore_index=True, inplace=True)
        return df
