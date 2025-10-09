"""
ETL routines for data from IHME.
See https://www.healthdata.org.
"""

from pathlib import Path

import country_converter as coco
import pandas as pd
from pydantic import Field

from ..storage import BaseStorage
from ..utils import generate_id
from ..validation import PREFIX_DISAGGREGATION, SexEnum
from ._base import BaseRetriever, BaseTransformer

BASE_URL = "https://www.healthdata.org/"


__all__ = ["Retriever", "Transformer"]


class Retriever(BaseRetriever):
    uri: Path = Field(
        default="inputs/IHME-GBD_2021_DATA-c13547d7-1.csv",
        frozen=True,
        validate_default=True,
        description="Path to a file downloaded from IHME's website.",
    )
    storage: BaseStorage

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the IHME.

        Parameters
        ----------
        **kwargs
            Extra arguments to pass to `storage.read_dataset`.

        Returns
        -------
        pd.DataFrame
            Raw data from the API for the indicators with supported disaggregations.
        """
        file_path = self.storage.join_path(self.uri)
        return self.storage.read_dataset(file_path, **kwargs)


class Transformer(BaseTransformer):

    def __call__(self, df: pd.DataFrame, **kwargs):
        """
        Transform raw data from IHME.

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
        df["source"] = BASE_URL
        df["country_code"] = cc.pandas_convert(df["location_name"], to="ISO3")
        # construct indicator names and derive indicator codes
        df["indicator_name"] = df["measure_name"] + ", " + df["cause_name"]
        df["indicator_code"] = df["indicator_name"].apply(generate_id)

        # recode sex columns
        mapping = {
            "Male": SexEnum.MALE.value,
            "Female": SexEnum.FEMALE.value,
            "Both sexes": SexEnum.BOTH.value,
            "All sexes": SexEnum.TOTAL.value,
        }
        df["sex_name"] = df["sex_name"].replace(mapping)

        # rename columns
        mapping = {
            "val": "value",
            "metric_name": "unit",
            "sex_name": PREFIX_DISAGGREGATION + "sex",
            "age_name": PREFIX_DISAGGREGATION + "age",
            "cause_name": PREFIX_DISAGGREGATION + "cause",
        }
        df.rename(columns=mapping, inplace=True)
        return df
