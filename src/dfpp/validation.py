"""
Validation function to ensure data integrity and consistency.
"""

import pandas as pd

__all__ = ["check_duplicates"]


def check_duplicates(df: pd.DataFrame) -> None:
    """
    Check if the canonical data frame contains any duplicates.

    Returns
    -------
    None
        If the validation has passed.

    Raises
    ------
    AssertionError
        If any duplicates found.
    """
    panel_columns = ["alpha_3_code", "series_id", "year"]
    disaggregation_columns = df.filter(like="disagr_", axis=1).columns.tolist()
    columns = panel_columns + disaggregation_columns
    assert (n := df.duplicated(subset=columns).sum()) == 0, f"{n:,} duplicates found."
