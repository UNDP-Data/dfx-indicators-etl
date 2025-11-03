"""
Validation function to ensure data integrity and consistency.
"""

from enum import StrEnum

import pandas as pd
import pandera.pandas as pa
from pandera.typing.pandas import Series

from .utils import _combine_disaggregations

__all__ = ["count_duplicates", "check_duplicates", "MetadataSchema", "schema"]

PREFIX_DISAGGREGATION = "disagr_"
PREFIX_PROPERTY = "prop_"


class SexEnum(StrEnum):
    """
    Standardised names for sex disaggregation categories.
    """

    MALE = "Male"
    FEMALE = "Female"
    BOTH = "Both"
    OTHER = "Other"
    TOTAL = "Total"
    NOT_APPLICABLE = "Not applicable"
    UNKNOWN = "Unknown"
    NON_RESPONSE = "Non response"


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
    columns = ["indicator_name", "country_code", "year", "disaggregation"]
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


class MetadataSchema(pa.DataFrameModel):
    """
    Indicator metadata schema.
    """

    code: Series[str] = pa.Field(
        str_length={"min_value": 1, "max_value": 128}, nullable=False
    )
    name: Series[str] = pa.Field(
        str_length={"min_value": 2, "max_value": 512}, nullable=False
    )
    unit: Series[str] = pa.Field(
        str_length={"min_value": 1, "max_value": 128}, nullable=True
    )

    class Config:
        name = "IndicatorMetadataSchema"
        strict = "filter"
        add_missing_columns = True

    @pa.parser("code", "name", "unit")
    def strip(cls, series):
        return series.str.strip()

    @pa.dataframe_check
    def no_duplicates(cls, df: pd.DataFrame) -> bool:
        return df.duplicated().sum() == 0


schema = pa.DataFrameSchema(
    columns={
        "source": pa.Column(
            dtype=str,
            checks=[pa.Check.str_length(min_value=2, max_value=1024)],
            nullable=False,
            description="Source of the data. Typically a URL",
        ),
        "indicator_name": pa.Column(
            dtype=str,
            checks=[pa.Check.str_length(min_value=2, max_value=512)],
            nullable=False,
        ),
        "country_code": pa.Column(
            dtype=str,
            checks=[pa.Check.str_matches(r"^[A-Z]{3}$")],
            nullable=False,
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
        "disaggregation": pa.Column(
            dtype=str,
            nullable=False,
            required=False,
        ),
        rf"^{PREFIX_PROPERTY}*": pa.Column(
            dtype=str,
            nullable=True,
            regex=True,
            required=False,
        ),
    },
    parsers=pa.Parser(lambda df: _combine_disaggregations(df, PREFIX_DISAGGREGATION)),
    checks=[
        pa.Check(
            lambda df: count_duplicates(df) == 0,
            error="Duplicate rows found.",
            name="Check duplicates",
        )
    ],
    strict="filter",
    name="Standardised Data Frame",
    add_missing_columns=True,
    description="Standardised data frame to be returned by a transformer",
)
