"""
Functions to transform raw data from WHO GHO API.
"""

import pandas as pd

from dfpp.transformation.column_name_template import (
    ensure_canonical_columns,
    sort_columns_canonically,
)

from ...utils import to_snake_case
from ...validation import check_duplicates


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform data from the WHO GHO API.

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
        "IndicatorCode": "series_id",
        "SpatialDim": "alpha_3_code",
        "TimeDim": "year",
        "NumericValue": "value",
        "DataSourceDim": "source",  # keep the original source as to avoid duplicates
    }

    # "unstack" dimensions from the long format for Dim1, Dim2 etc columns
    for column in df.filter(regex=r"Dim\dType"):
        dimensions = sorted(df[column].dropna().unique())
        for dimension in dimensions:
            column_dim = f"disagr_{dimension}"
            if column_dim not in df.columns:
                df[column_dim] = None
            mask = df[column].eq(dimension)
            df.loc[mask, column_dim] = df.loc[mask, column_dim].combine_first(
                df.loc[mask, column.replace("Type", "")]
            )
    df = (
        df.reindex(columns=columns)
        .rename(columns=columns)
        .join(df.filter(like="disagr_", axis=1).rename(to_snake_case, axis=1))
    )
    df["source"] = df["source"].apply(lambda x: "https://who.int" + f" | {x}")
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    check_duplicates(df)
    return df.reset_index(drop=True)
