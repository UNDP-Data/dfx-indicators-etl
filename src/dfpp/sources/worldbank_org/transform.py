"""
Functions to transform raw data from the World Bank Indicator API.
"""

import country_converter as coco
import pandas as pd

from dfpp.transformation.column_name_template import (
    ensure_canonical_columns,
    sort_columns_canonically,
)

from ...validation import check_duplicates


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform data from the World Bank Indicator API.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data frame.

    Returns
    -------
    pd.DataFrame
        Transformed data frame in the canonical format.
    """
    df = df.copy()
    for column in ("indicator", "country"):
        df = df.join(
            pd.DataFrame(df[column].tolist()).rename(lambda x: f"{column}_{x}", axis=1)
        )
        df.drop(column, axis=1, inplace=True)
    df.replace({"": None}, inplace=True)
    cc = coco.CountryConverter()
    df["country_value"] = cc.pandas_convert(
        df["country_value"], to="ISO3", not_found=None
    )

    for column in ("country_id", "country_value"):
        df["countryiso3code"] = df["countryiso3code"].combine_first(df[column])
    df.dropna(subset=["countryiso3code"], inplace=True)

    # keep only yearly data
    df = df.loc[df["date"].str.isdigit()].copy()

    columns = {
        "indicator_id": "series_id",
        "indicator_value": "series_name",
        "countryiso3code": "alpha_3_code",
        "date": "year",
        "value": "value",
    }

    df = df.reindex(columns=columns).rename(columns=columns)
    df["source"] = "https://api.worldbank.org"
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    check_duplicates(df)
    return df.reset_index(drop=True)
