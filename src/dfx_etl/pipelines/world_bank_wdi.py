"""
ETL components to process data from the World Development Indicators by  the World Bank.
See https://datatopics.worldbank.org/world-development-indicators/.
"""

import logging
from pathlib import Path

import pandas as pd
from pydantic import Field

from ..storage import BaseStorage
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]

logger = logging.getLogger(__name__)


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the WDI database.

    Use bulk download to manually obtain the data first.
    """

    uri: Path = Field(
        default="inputs/WDI_CSV/WDICSV.csv",
        frozen=True,
        validate_default=True,
    )

    def __call__(self, storage: BaseStorage, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the WDI database files.

        Parameters
        ----------
        storage : BaseStorage
            Storage to retrieve the data file from.
        **kwargs
            Extra arguments to pass to `pd.read_*` function.

        Returns
        -------
        pd.DataFrame
            Raw data frame with the data from the databae.
        """
        return storage.read_dataset(self.uri, **kwargs)

    def _get_metadata(self, storage: BaseStorage) -> pd.DataFrame:
        """
        Get the metadata file.
        """
        file_path = storage.join_path(self.uri.with_name("WDISeries.csv"))
        return storage.read_dataset(file_path)


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the World Bank Indicator API.
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
        columns = {
            "Country Name": "country_name",
            "Country Code": "country_code",
            "Indicator Name": "indicator_name",
            "Indicator Code": "indicator_code",
        }
        df = df.melt(id_vars=list(columns), var_name="year", value_name="value")
        df["year"] = df["year"].astype(int)
        df = df.query("year >= 2015").dropna(subset=["value"])
        df.rename(columns=columns, inplace=True)
        df["indicator_name"] = df.apply(
            lambda row: f"{row.indicator_name} [{row.indicator_code}]", axis=1
        )
        return df
