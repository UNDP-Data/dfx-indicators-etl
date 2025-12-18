"""
Validation function to ensure data integrity and consistency.
"""

from enum import StrEnum

import pandas as pd
import pandera.pandas as pa
from pandera.typing.pandas import Series

from .utils import _combine_disaggregations

__all__ = ["MetadataSchema", "DataSchema"]

PREFIX_DISAGGREGATION = "disagr_"


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
        unique = ["code", "name", "unit"]

    @pa.parser("code", "name", "unit")
    @classmethod
    def strip(cls, series):
        return series.str.strip()


class DataSchema(pa.DataFrameModel):
    """
    Indicator data schema.
    """

    source: Series[str] = pa.Field(
        str_length={"min_value": 2, "max_value": 1024},
        nullable=False,
        description="Source of the data. Typically a URL",
    )
    indicator_name: Series[str] = pa.Field(
        str_length={"min_value": 2, "max_value": 512},
        nullable=False,
    )
    country_code: Series[str] = pa.Field(
        str_matches=r"^[A-Z]{3}$",
        nullable=False,
        description="ISO 3166-1 alpha-3 three-letter country code",
    )
    year: Series[int] = pa.Field(
        ge=1900,
        le=2100,
        nullable=False,
        coerce=True,
    )
    disaggregation: Series[str] = pa.Field(nullable=False)
    value: Series[float] = pa.Field(
        nullable=False,
        coerce=True,
    )

    class Config:
        name = "IndicatorDataSchema"
        strict = "filter"
        add_missing_columns = True
        unique = ["indicator_name", "country_code", "year", "disaggregation"]

    @pa.dataframe_parser
    @classmethod
    def combine_disaggregations(cls, df: pd.DataFrame) -> pd.DataFrame:
        return _combine_disaggregations(df, PREFIX_DISAGGREGATION)
