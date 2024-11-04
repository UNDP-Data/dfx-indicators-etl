"""store basic column name and column value conventions"""

import logging
import re
from enum import Enum, StrEnum

import pandas as pd

DIMENSION_COLUMN_PREFIX = "disagr_"
DIMENSION_COLUMN_CODE_SUFFIX = "_code"
DIMENSION_COLUMN_NAME_SUFFIX = "_name"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

CANONICAL_COLUMN_NAMES = [
    "source",
    "series_id",
    "series_name",
    "alpha_3_code",
    DIMENSION_COLUMN_PREFIX + "unit" + DIMENSION_COLUMN_CODE_SUFFIX,
    DIMENSION_COLUMN_PREFIX + "unit" + DIMENSION_COLUMN_NAME_SUFFIX,
    DIMENSION_COLUMN_PREFIX + "observation_type" + DIMENSION_COLUMN_CODE_SUFFIX,
    DIMENSION_COLUMN_PREFIX + "observation_type" + DIMENSION_COLUMN_NAME_SUFFIX,
    "year",
    "value",
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


def set_dimension_column_prefix(dimension_columns: str):
    return dimension_columns


def sort_columns_canonically(df):
    assert all(
        col in df.columns for col in CANONICAL_COLUMN_NAMES
    ), f"DataFrame does not contain all canonical columns. Missing columns: {set(CANONICAL_COLUMN_NAMES) - set(df.columns)}"

    canonical_cols_start = [col for col in CANONICAL_COLUMN_NAMES if col in df.columns][
        :4
    ]
    canonical_cols_end = [col for col in CANONICAL_COLUMN_NAMES if col in df.columns][
        4:
    ]

    grouped_disagr_cols = get_grouped_disagr_columns(df)

    other_cols = [
        col
        for col in df.columns
        if col not in CANONICAL_COLUMN_NAMES and col not in grouped_disagr_cols
    ]

    sorted_columns = (
        canonical_cols_start + grouped_disagr_cols + other_cols + canonical_cols_end
    )

    return df[sorted_columns]


def get_grouped_disagr_columns(df: pd.DataFrame) -> list:
    """Returns the grouped disagr columns in the DataFrame
    so that _code and _name suffixed dimension columns are displayed next to each other.
    """
    disagr_cols = [
        col
        for col in df.columns
        if col.startswith("disagr_") and col not in CANONICAL_COLUMN_NAMES
    ]

    if not disagr_cols:
        return []

    grouped_disagr_cols = []
    base_names = set(re.sub(r"(_code|_name)$", "", col) for col in disagr_cols)

    for base in sorted(base_names):
        code_col = f"{base}_code"
        name_col = f"{base}_name"
        if code_col in df.columns:
            grouped_disagr_cols.append(code_col)
        if name_col in df.columns:
            grouped_disagr_cols.append(name_col)

    return grouped_disagr_cols


def ensure_canonical_columns(df: pd.DataFrame) -> pd.DataFrame:
    """if any of the canonical is absent, set it"""
    for column in CANONICAL_COLUMN_NAMES:
        if column not in df.columns:
            logging.warning(f"Filling missing canonical column {column} with None")
            df[column] = None
    return df
