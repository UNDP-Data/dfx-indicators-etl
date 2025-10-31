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
        description="See https://www.sipri.org/databases/milex.",
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
        data = []
        for sheet_name, indicator_name in tqdm(self.metadata.items()):
            df = self._get_data(sheet_name)
            if df is None:
                continue
            df["indicator_name"] = indicator_name
            data.append(df)
        return pd.concat(data, axis=0, ignore_index=True)

    @property
    def metadata(self) -> dict[str, str]:
        """
        Indicator metadata for sheets in the Excel file.

        Returns
        -------
        dict[str, str]
            Mapping of sheet names to indicator names
        """
        return {
            "Current US$": "Military expenditure by country in $current US m., presented according to calendar year [SIPRI_MILEXT_CURRENT_USD]",
            "Share of GDP": "Military expenditure by country as a share of gross domestic product (GDP), presented according to calendar year [SIPRI_MILEXT_SHARE_OF_GDP]",
            "Per capita": "Military expenditure per capita, in current US$, presented according to calendar year, 1988-2024 only, [SIPRI_MILEXT_PER_CAPITA]",
            "Share of Govt. spending": "Military expenditure as a percentage of general government expenditure, 1988-2024 only [SIPRI_MILEXT_SHARE_OF_GOV_SPENDING]",
        }

    def _get_data(self, sheet_name: str) -> pd.DataFrame:
        """
        Get series data from the the SIPRI Military Expenditure Database.

        Parameters
        ----------
        sheet_name : str
            Sheet name to read from the Excel file. See `metadata`.

        Returns
        -------
        pd.DataFrame
            Data frame with country data in the wide format.
        """
        # infer the header row
        xlsx = pd.ExcelFile(str(self.uri))
        df = xlsx.parse(sheet_name=sheet_name)
        header = df.iloc[:, 0].eq("Country").idxmax() + 1
        return xlsx.parse(
            sheet_name=sheet_name, header=header, na_values=["xxx", "..."]
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
        columns = ["Country", "indicator_name"]
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
