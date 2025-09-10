"""
Validation function to ensure data integrity and consistency.
"""

import pandas as pd

__all__ = ["count_duplicates", "check_duplicates"]


def count_duplicates(df: pd.DataFrame) -> int:
    """
    Count duplicates in a canonical data frame.

    Parameters
    ----------
    df : pd.DataFrame
        Canonical data frame.

    Returns
    -------
    int
        Count of duplicates in the data frame.
    """
    panel_columns = ["alpha_3_code", "series_id", "year"]
    disaggregation_columns = df.filter(like="disagr_", axis=1).columns.tolist()
    columns = panel_columns + disaggregation_columns
    return df.reindex(columns=columns).duplicated().sum()


def check_duplicates(df: pd.DataFrame) -> None:
    """
    Check if the canonical data frame contains any duplicates.

    Parameters
    ----------
    df : pd.DataFrame
        Canonical data frame.

    Returns
    -------
    None
        If the validation has passed.

    Raises
    ------
    AssertionError
        If any duplicates found.
    """
    assert (n := count_duplicates(df)) == 0, f"{n:,} duplicates found."
