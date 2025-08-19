"""
Transform raw data from SIPRI.
"""

import country_converter as coco
import pandas as pd

from ...transformation.column_name_template import (
    ensure_canonical_columns,
    sort_columns_canonically,
)
from ...validation import check_duplicates
from .retrieve import DF_METADATA

__all__ = ["transform"]


def transform(df: pd.DataFrame) -> pd.DataFrame:
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
    df = df[["Country", "series_id"]].join(df.filter(regex="\d+"))
    # Reshape from wide to long
    df = df.melt(id_vars=["Country", "series_id"], var_name="year", value_name="value")
    # Remove missing values
    df = df.dropna(ignore_index=True)
    # Infer country ISO alpha-3 codes from names
    cc = coco.CountryConverter()
    df["alpha_3_code"] = cc.pandas_convert(df["Country"], to="ISO3", not_found=None)
    df = df.dropna(subset="alpha_3_code")
    df = df.drop(columns=["Country"])
    # Standardise the dataframe
    df = df.merge(DF_METADATA, how="left", on="series_id")
    df["source"] = "https://www.sipri.org/databases/milex"
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    # check_duplicates(df)
    return df.reset_index(drop=True)
