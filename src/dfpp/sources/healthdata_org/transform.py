"""transform healthdata.org files manually downloaded and stored in blob storage"""

import string

import country_converter as coco
import pandas as pd

from dfpp.transformation import exceptions
from dfpp.transformation.column_name_template import (
    CANONICAL_COLUMN_NAMES,
    DIMENSION_COLUMN_PREFIX,
    SERIES_PROPERTY_PREFIX,
    SexEnum,
    ensure_canonical_columns,
    sort_columns_canonically,
)
from dfpp.transformation.value_handler import handle_value

BASE_URL = "https://www.healthdata.org/"


SEX_VALUES_TO_REPLACE = {
    "Male": SexEnum.MALE.value,
    "Female": SexEnum.FEMALE.value,
    "Both sexes": SexEnum.BOTH.value,
    "All sexes": SexEnum.TOTAL.value,
}


PRIMARY_COLUMNS_TO_RENAME = {
    "val": "value",
    "metric_name": SERIES_PROPERTY_PREFIX + "unit",
    "sex_name": DIMENSION_COLUMN_PREFIX + "sex",
    "age_name": DIMENSION_COLUMN_PREFIX + "age",
    "cause_name": DIMENSION_COLUMN_PREFIX + "cause",
}


def transform_series(df: pd.DataFrame, coco: coco.CountryConverter) -> pd.DataFrame:
    """
    Transform a DataFrame containing multiple series into a structured format.

    Args:
        df (pd.DataFrame): The input DataFrame containing series data.
        coco (coco.CountryConverter): A CountryConverter instance for converting country names to ISO3 codes.

    Returns:
        pd.DataFrame: The transformed DataFrame with structured series data.
    """

    df["source"] = BASE_URL
    df["alpha_3_code"] = coco.pandas_convert(df["location_name"], to="ISO3")

    df["series_name"] = df["measure_name"] + ", " + df["cause_name"]

    df["series_id"] = (
        df["series_name"]
        .str.replace(f"[{string.punctuation}]", "", regex=True)
        .str.replace(" ", "_")
        .str.lower()
    )

    df.rename(
        columns=PRIMARY_COLUMNS_TO_RENAME,
        inplace=True,
    )

    df[DIMENSION_COLUMN_PREFIX + "sex"] = df[DIMENSION_COLUMN_PREFIX + "sex"].replace(
        SEX_VALUES_TO_REPLACE
    )
    assert (
        df[DIMENSION_COLUMN_PREFIX + "sex"].isna().any() == False
    ), exceptions.DIMENSION_REMAP_ERROR_MESSAGE

    disagr_columns = [
        col
        for col in df.columns
        if col.startswith(DIMENSION_COLUMN_PREFIX) and col not in CANONICAL_COLUMN_NAMES
    ]

    property_columns = [
        col
        for col in df.columns
        if col.startswith(SERIES_PROPERTY_PREFIX) and col not in CANONICAL_COLUMN_NAMES
    ]
    df[["value", SERIES_PROPERTY_PREFIX + "value_label"]] = df.apply(
        handle_value, axis=1, result_type="expand"
    )
    df = ensure_canonical_columns(df)
    df = df[CANONICAL_COLUMN_NAMES + disagr_columns + property_columns]
    df = sort_columns_canonically(df)
    assert (
        df.drop("value", axis=1).duplicated().sum() == 0
    ), exceptions.DUPLICATE_ERROR_MESSAGE
    return df
