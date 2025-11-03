"""
ETL components to process data from the UNAIDS Key Population Atlas
by Joint United Nations Programme on HIV and AIDS (UNAIDS).
See https://kpatlas.unaids.org.
"""

from pathlib import Path

import pandas as pd
from pydantic import Field

from ..storage import BaseStorage
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the UNAIDS Key Population Atlas.
    """

    uri: Path = Field(
        default="inputs/KPAtlasDB_2025_en.csv",
        frozen=True,
        validate_default=True,
        description="""Dataset file downloaded from the UNAIDS Key Population Atlas,
        e.g.,https://aidsinfo.unaids.org/public/documents/KPAtlasDB_2025_en.zip""",
    )
    storage: BaseStorage

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the UNAIDS Key Population Atlas.

        Parameters
        ----------
        **kwargs
            Extra arguments to pass to `pd.read_csv`.

        Returns
        -------
        pd.DataFrame
            Raw data frame with data from the dashboard.
        """
        file_path = self.storage.join_path(self.uri)
        return self.storage.read_dataset(file_path, **kwargs)


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the UNAIDS Key Population Atlas.
    """

    def transform(self, df: pd.DataFrame, **kwargs):
        """
        Transform data from UNAIDS Key Population Atlas.

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
            "Indicator": "indicator_name",
            "Area ID": "country_code",
            "Time Period": "year",
            "Data value": "value",
            "Source": "source",
        }
        # remove unspecified disaggregations
        df = df.loc[~df["Subgroup"].str.startswith("Category")].copy()
        # only keep indicators with just one or 'Total' disaggregation
        df["n_subgroups"] = df.groupby("Indicator")["Subgroup"].transform("nunique")
        df = df.loc[df["n_subgroups"].eq(1) | df["Subgroup"].eq("Total")].copy()
        df["indicator_name"] = df.apply(
            lambda row: f"{row.Indicator.strip()}, {row.Unit.strip()}", axis=1
        )
        df = df.reindex(columns=columns).rename(columns=columns)
        # remove all duplicates
        df.drop_duplicates(
            subset=["indicator_name", "country_code", "year"],
            keep=False,
            ignore_index=True,
            inplace=True,
        )
        # remove rows without values
        df.dropna(subset=["value"], inplace=True)
        return df
