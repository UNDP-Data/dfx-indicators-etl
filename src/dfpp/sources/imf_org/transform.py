"""
Transform raw data from the IMF.
"""

import pandas as pd

from ...transformation.column_name_template import (
    ensure_canonical_columns,
    sort_columns_canonically,
)
from ...validation import check_duplicates
from .retrieve import get_series_metadata

__all__ = ["transform"]


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tranform function for raw IMF data.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data as returned by the retrieval section.

    Returns
    -------
    pd.DataFrame
        Standardised data frame.
    """
    df = df.merge(get_series_metadata(), how="left", on="series_id")
    df["source"] = "https://www.imf.org/external/datamapper"
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    check_duplicates(df)
    return df.reset_index(drop=True)
