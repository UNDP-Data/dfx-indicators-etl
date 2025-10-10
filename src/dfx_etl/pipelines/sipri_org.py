"""
ETL components to process data from the SIPRI Military Expenditure Database
by Stockholm International Peace Research Institute (SIPRI).
See https://www.sipri.org/databases/milex.
"""

import country_converter as coco
import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm

from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the SIPRI Milex.
    """

    uri: HttpUrl = Field(
        default="https://www.sipri.org/sites/default/files/SIPRI-Milex-data-1949-2024_2.xlsx",
        frozen=True,
        validate_default=True,
        examples=[
            "https://www.sipri.org/sites/default/files/SIPRI-Milex-data-1948-2023.xlsx"
        ],
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
        df_metadata = self._get_metadata()
        data = []
        for indicator_code in tqdm(df_metadata["indicator_code"]):
            df = self._get_data(indicator_code)
            if df is None:
                continue
            df["indicator_code"] = indicator_code
            data.append(df)
        df_data = pd.concat(data, axis=0, ignore_index=True)
        df_data = df_data.merge(df_metadata, how="left", on="indicator_code")
        return df_data

    def _get_metadata(self) -> pd.DataFrame:
        """
        Get series metadata for sheets in the Excel file.

        Returns
        -------
        pd.DataFrame
            Data frame with twthree columns: `indicator_code`, `indicator_name` and `unit`.
        """
        return pd.DataFrame(
            [
                (
                    "Current US$",
                    "SIPRI_MILEXT_CURRENT_USD",
                    "Military expenditure by country in current US$ m., presented according to calendar year.",
                    "Million USD (current)",
                ),
                (
                    "Share of GDP",
                    "SIPRI_MILEXT_SHARE_OF_GDP",
                    "Military expenditure by country as a share of gross domestic product (GDP), presented according to calendar year.",
                    "Share of GDP",
                ),
                (
                    "Per capita",
                    "SIPRI_MILEXT_PER_CAPITA",
                    "Military expenditure per capita, in current US$, presented according to calendar year (1988-2024 only).",
                    "USD (current)",
                ),
                (
                    "Share of Govt. spending",
                    "SIPRI_MILEXT_SHARE_OF_GOV_SPENDING",
                    "Military expenditure as a percentage of general government expenditure (1988-2024 only).",
                    "Percent of Government Spending",
                ),
            ],
            columns=["sheet_name", "indicator_code", "indicator_name", "unit"],
        )

    def _get_data(self, indicator_code: str) -> pd.DataFrame:
        """
        Get series data from the the SIPRI Military Expenditure Database.

        Parameters
        ----------
        series_id : str
            Series ID. See `get_series_metadata`.

        Returns
        -------
        pd.DataFrame or None
            Data frame with country data in the wide format.
        """
        df_metadata = self._get_metadata()
        mapping = dict(df_metadata[["indicator_code", "sheet_name"]].values)
        if (sheet_name := mapping[indicator_code]) is None:
            return None
        # Infer the header row
        df = pd.read_excel(str(self.uri), sheet_name=sheet_name)
        header = df.iloc[:, 0].eq("Country").idxmax() + 1
        return pd.read_excel(
            str(self.uri),
            sheet_name=sheet_name,
            header=header,
            na_values=["xxx", "..."],
        )


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the SIPRI Milex.
    """

    def __call__(self, df: pd.DataFrame, **kwargs):
        """
        Tranform function for raw SIPRI data.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data as returned by the retrieval section.

        Returns
        -------
        pd.DataFrame
            Standardised data frame.
        """

        # Subset only relevant columns
        columns = ["Country", "indicator_code", "indicator_name", "unit"]
        df = df[columns].join(df.filter(regex=r"\d+"))
        # Reshape from wide to long
        df = df.melt(id_vars=columns, var_name="year", value_name="value")
        # Remove missing values
        df = df.dropna(ignore_index=True)
        # Infer country ISO alpha-3 codes from names
        cc = coco.CountryConverter()
        df["country_code"] = cc.pandas_convert(df["Country"], to="ISO3", not_found=None)
        df = df.dropna(subset="country_code")
        df = df.drop(columns=["Country"])
        return df.reset_index(drop=True)
