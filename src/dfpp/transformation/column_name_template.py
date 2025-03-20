"""store basic column name and column value conventions"""

import logging
from enum import Enum, StrEnum

import pandas as pd

DIMENSION_COLUMN_PREFIX = "disagr_"
SERIES_PROPERTY_PREFIX = "prop_"

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

CANONICAL_COLUMN_NAMES = [
    "source",
    "series_id",
    "series_name",
    "alpha_3_code",
    SERIES_PROPERTY_PREFIX + "unit",
    SERIES_PROPERTY_PREFIX + "observation_type",
    "year",
    "value",
    SERIES_PROPERTY_PREFIX + "value_label",
]


class SexEnum(StrEnum, Enum):
    MALE = "male"
    FEMALE = "female"
    BOTH = "both"
    OTHER = "other"
    TOTAL = "total"
    NOT_APPLICABLE = "not applicable"
    UNKNOWN = "unknown"
    NON_RESPONSE = "non response"


def sort_columns_canonically(df):
    assert all(
        col in df.columns for col in CANONICAL_COLUMN_NAMES
    ), f"DataFrame does not contain all canonical columns. Missing columns: {set(CANONICAL_COLUMN_NAMES) - set(df.columns)}"

    canonical_cols_start = [col for col in CANONICAL_COLUMN_NAMES if col in df.columns][
        :3
    ]
    canonical_cols_end = [col for col in CANONICAL_COLUMN_NAMES if col in df.columns][
        3:
    ]

    grouped_disagr_cols = [
        col
        for col in df.columns
        if col.startswith(DIMENSION_COLUMN_PREFIX) and col not in CANONICAL_COLUMN_NAMES
    ]

    grouped_property_cols = [
        col
        for col in df.columns
        if col.startswith(SERIES_PROPERTY_PREFIX) and col not in CANONICAL_COLUMN_NAMES
    ]

    other_cols = [
        col
        for col in df.columns
        if all(
            [
                col not in CANONICAL_COLUMN_NAMES,
                col not in grouped_disagr_cols,
                col not in grouped_property_cols,
            ]
        )
    ]

    sorted_columns = (
        canonical_cols_start
        + grouped_disagr_cols
        + other_cols
        + grouped_property_cols
        + canonical_cols_end
    )

    return df[sorted_columns]


def ensure_canonical_columns(df: pd.DataFrame) -> pd.DataFrame:
    """if any of the canonical is absent, set it"""
    for column in CANONICAL_COLUMN_NAMES:
        if column not in df.columns:
            logging.warning(f"Filling missing canonical column {column} with None")
            df[column] = None
    return df
