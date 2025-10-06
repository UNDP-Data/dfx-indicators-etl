"""
Functions to transform raw data from UNAIDS Key Population Atlas.
"""

import pandas as pd

from dfpp.transformation.column_name_template import (
    ensure_canonical_columns,
    sort_columns_canonically,
)

from ...utils import generate_id
from ...validation import check_duplicates


def transform(df: pd.DataFrame) -> pd.DataFrame:
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
        "Indicator": "series_name",
        "Area ID": "alpha_3_code",
        "Time Period": "year",
        "Data value": "value",
        "Unit": "prop_unit",
        "Source": "source",
    }
    # Remove unspecified disaggregations
    df = df.loc[~df["Subgroup"].str.startswith("Category")].copy()
    # Only keep indicators with just one or 'Total' disaggregation
    df["n_subgroups"] = df.groupby("Indicator")["Subgroup"].transform("nunique")
    df = df.loc[df["n_subgroups"].eq(1) | df["Subgroup"].eq("Total")].copy()
    df = df.reindex(columns=columns).rename(columns=columns)
    df["source"] = "https://kpatlas.unaids.org"
    # Remove all duplicates
    df.drop_duplicates(
        subset=["series_name", "alpha_3_code", "year"],
        keep=False,
        ignore_index=True,
        inplace=True,
    )
    df["series_name"] = df["series_name"].str.strip()
    df["series_id"] = df["series_name"].apply(lambda x: "UNAIDS_" + generate_id(x))
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    check_duplicates(df)
    return df.reset_index(drop=True)
