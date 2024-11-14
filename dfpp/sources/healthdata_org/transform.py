"""transform healthdata.org files manually downloaded and stored in blob storage"""

import pandas as pd
import country_converter as coco
import string


from dfpp.transformation.column_name_template import (
    CANONICAL_COLUMN_NAMES,
    SERIES_PROPERTY_PREFIX,
    DIMENSION_COLUMN_PREFIX,
    SexEnum,
    sort_columns_canonically,
    ensure_canonical_columns,
)

BASE_URL = "https://www.healthdata.org/"


def transform_series(df: pd.DataFrame, coco: coco.CountryConverter) -> pd.DataFrame:
    """transform a dataframe containing multiple series into multiple series"""

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
        columns={
            "val": "value",
            "metric_name": SERIES_PROPERTY_PREFIX + "unit",
            "sex_name": DIMENSION_COLUMN_PREFIX + "sex",
            "age_name": DIMENSION_COLUMN_PREFIX + "age",
            "cause_name": DIMENSION_COLUMN_PREFIX + "cause",
        },
        inplace=True,
    )

    df[DIMENSION_COLUMN_PREFIX + "sex"] = df[DIMENSION_COLUMN_PREFIX + "sex"].map(
        {
            "Male": SexEnum.MALE.value,
            "Female": SexEnum.FEMALE.value,
            "Both sexes": SexEnum.BOTH.value,
            "All sexes": SexEnum.TOTAL.value,
        }
    )

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

    df = ensure_canonical_columns(df)
    df = df[CANONICAL_COLUMN_NAMES + disagr_columns + property_columns]
    df = sort_columns_canonically(df)
    assert df.drop("value", axis=1).duplicated().sum() == 0
    return df
