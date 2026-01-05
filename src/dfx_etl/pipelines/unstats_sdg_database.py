"""
ETL components to process data from the Global SDG Database by the UN Stats.
The pipeline is designed to work with manually exported Excel files in that the SDG API's
performance is lacking.

See https://unstats.un.org/sdgs/dataportal/database.
"""

import logging
from pathlib import Path

import pandas as pd
from pydantic import Field
from tqdm import tqdm

from ..exceptions import StorageRequiredError
from ..utils import replace_country_metadata, to_snake_case
from ..validation import PREFIX_DISAGGREGATION
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]

logger = logging.getLogger(__name__)


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the Global SDG database.

    Use bulk download to manually obtain the data first.
    """

    uri: Path = Field(
        default="inputs/SDG Database",
        frozen=True,
        validate_default=True,
    )

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the WDI database files.

        Parameters
        ----------
        **kwargs
            Extra arguments to pass to `pd.read_*` function.

        Returns
        -------
        pd.DataFrame
            Raw data frame with the data from the databae.
        """
        if (storage := kwargs.pop("storage", None)) is None:
            raise StorageRequiredError
        data = []
        # All 17 SDGs
        for goal in tqdm(range(1, 18)):
            df = storage.read_dataset(self.uri.joinpath(f"Goal{goal}.xlsx"), **kwargs)
            data.append(df)
        return pd.concat(data, axis=0, ignore_index=True)


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the Global SDG database.
    """

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform data from the WDI dataset.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data frame.

        Returns
        -------
        pd.DataFrame
            Transformed data frame in the canonical format.
        """
        # Non-disaggregation columns
        columns = {
            "Goal": None,
            "Target": None,
            "Indicator": None,
            "SeriesCode": "indicator_code",
            "SeriesDescription": "indicator_name",
            "GeoAreaCode": "country_code",
            "GeoAreaName": None,
            "TimePeriod": "year",
            "Value": "value",
            "Time_Detail": None,
            "TimeCoverage": None,
            "UpperBound": None,
            "LowerBound": None,
            "BasePeriod": None,
            "Source": None,
            "GeoInfoUrl": None,
            "FootNote": None,
            "Nature": None,
            "Reporting Type": None,
            "Units": None,
        }
        # Infer diaggregation columns which differ depending on the SDG
        disaggregations = list(set(df.columns) - set(columns))
        # Filter out the columns and create a mapping for renaming
        columns = {k: v for k, v in columns.items() if v is not None}
        columns |= {
            column: to_snake_case(column, prefix=PREFIX_DISAGGREGATION)
            for column in disaggregations
        }
        df = df.reindex(columns=columns).rename(columns=columns)
        df["indicator_name"] = df.apply(
            lambda row: f"{row.indicator_name} [{row.indicator_code}]", axis=1
        )
        df.drop(columns=["indicator_code"], inplace=True)
        df["country_code"] = replace_country_metadata(
            df["country_code"].astype(str), "m49", "iso-alpha-3"
        )
        # Handle values like '<2.5' or '>99' by keeping the numeric part only
        df["value"] = pd.to_numeric(
            df["value"].astype(str).str.lstrip("<|>"), errors="coerce"
        )
        df.dropna(subset=["value"], ignore_index=True, inplace=True)
        # Drop full duplicates since indicators may be repeated for several Goals
        df.drop_duplicates(ignore_index=True, inplace=True)
        return df
