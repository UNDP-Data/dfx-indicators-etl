"""
Validation function to ensure data integrity and consistency.
"""

import pandas as pd
import pandera.pandas as pa

__all__ = ["count_duplicates", "check_duplicates", "schema"]

PREFIX_DISAGGREGATION = "disagr_"
PREFIX_PROPERTY = "prop_"


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
    panel_columns = ["indicator_code", "country_code", "year", "unit"]
    disaggregation_columns = df.filter(
        like=PREFIX_DISAGGREGATION, axis=1
    ).columns.tolist()
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


schema = pa.DataFrameSchema(
    columns={
        "source": pa.Column(
            dtype=str,
            checks=[pa.Check.str_length(min_value=2, max_value=1024)],
            nullable=False,
            description="Source of the data. Typically a URL",
        ),
        "indicator_code": pa.Column(
            dtype=str,
            checks=[pa.Check.str_length(min_value=2, max_value=128)],
            nullable=False,
        ),
        "indicator_name": pa.Column(
            dtype=str,
            checks=[pa.Check.str_length(min_value=2, max_value=128)],
            nullable=False,
        ),
        "country_code": pa.Column(
            dtype=str,
            checks=[pa.Check.str_matches(r"^[A-Z]{3}$")],
            nullable=False,
        ),
        "unit": pa.Column(
            dtype=str,
            checks=[pa.Check.str_length(min_value=2, max_value=128)],
            nullable=True,
        ),
        "year": pa.Column(
            dtype=int,
            checks=[pa.Check.between(1900, 2100)],
            nullable=False,
            coerce=True,
        ),
        "value": pa.Column(
            dtype=float,
            nullable=False,
            coerce=True,
        ),
        rf"^{PREFIX_DISAGGREGATION}*": pa.Column(
            dtype=str,
            nullable=False,
            regex=True,
            required=False,
        ),
        rf"^{PREFIX_PROPERTY}*": pa.Column(
            dtype=str,
            nullable=False,
            regex=True,
            required=False,
        ),
    },
    checks=[
        pa.Check(
            lambda df: count_duplicates(df) == 0,
            error="Duplicate rows found.",
            name="Check duplicates",
        )
    ],
    strict=True,
    name="Standardised Data Frame",
    description="Standardised data frame to be returned by a transformer",
)
