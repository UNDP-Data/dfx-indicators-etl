"""
transform series data retrieved via api into publishable format
"""

import logging
import pandas as pd

from dfpp.transformation.column_name_template import (
    sort_columns_canonically,
    ensure_canonical_columns,
    SERIES_PROPERTY_PREFIX,
    DIMENSION_COLUMN_PREFIX,
    CANONICAL_COLUMN_NAMES,
)
from dfpp.transformation.value_handler import handle_value
from dfpp.transformation import exceptions

from dfpp.sources.worldbank_org.retrieve import BASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


PRIMARY_COLUMNS_TO_RENAME = {
    "unit": SERIES_PROPERTY_PREFIX + "unit",
    "obs_status": SERIES_PROPERTY_PREFIX + "observation_type",
    "date": "year",
    "value": "value",
    "value_label": SERIES_PROPERTY_PREFIX + "value_label",
}

__all__ = ["transform_series"]


def transform_series(df: pd.DataFrame, iso_3_map: dict) -> pd.DataFrame:
    """
    Transform a DataFrame containing series data into a structured format.

    Args:
        df (pd.DataFrame): The input DataFrame containing series data.
        iso_3_map (dict): A dictionary mapping alpha-3 country codes to their corresponding names.
    Returns:
        pd.DataFrame: The transformed DataFrame with structured and canonical series data.
    """
    to_rename_columns = PRIMARY_COLUMNS_TO_RENAME.copy()
    # Normalize and rename indicator columns
    df_indicator = pd.json_normalize(df["indicator"])
    df_indicator.rename(
        columns={"id": "series_id", "value": "series_name"}, inplace=True
    )
    # Normalize and rename country columns
    df_country = pd.json_normalize(df["country"])
    df_country.rename(
        columns={"id": "alpha_3_code_id", "value": "country_name"}, inplace=True
    )

    # Concatenate normalized dataframes with original dataframe
    df = pd.concat(
        [df.drop(columns=["indicator", "country"]), df_indicator, df_country], axis=1
    )

    # Replace empty strings with None
    df.replace({"": None}, inplace=True)

    alpha_3_code_candidates = [
        "countryiso3code",
        "alpha_3_code_id",
    ]

    for col_name in alpha_3_code_candidates:
        if not df[col_name].isna().all():
            # if any unique value is alpha2 but not alpha3 code raise ValueError
            unique_values = df[col_name].unique()
            if any(len(value) == 2 for value in unique_values if value):
                raise ValueError(
                    f"Column {col_name} contains alpha-2 country codes which are not supported."
                )

            if any(value in iso_3_map.values() for value in unique_values if value):
                to_rename_columns[col_name] = "alpha_3_code"
                break

    if "alpha_3_code" not in to_rename_columns.values():
        df["alpha_3_code"] = df["country_name"].replace(iso_3_map)

    # Rename primary columns using predefined mapping
    df.rename(columns=to_rename_columns, inplace=True)

    assert (
        "alpha_3_code" in df.columns
    ), "alpha_3_code column not found (has not been assigned)"

    # Apply value handling to transform values and value labels
    df[["value", SERIES_PROPERTY_PREFIX + "value_label"]] = df.apply(
        handle_value, axis=1, result_type="expand"
    )

    # Select columns with specific prefixes that are not in canonical names
    to_select_columns = [
        col
        for col in df.columns
        if any(
            [
                col.startswith(DIMENSION_COLUMN_PREFIX),
                col.startswith(SERIES_PROPERTY_PREFIX),
            ]
        )
        and col not in CANONICAL_COLUMN_NAMES
    ]

    # Assign source URL to the DataFrame
    df["source"] = BASE_URL

    # Filter and organize dataframe columns
    df = df[CANONICAL_COLUMN_NAMES + to_select_columns]

    # Convert year to integer, selecting only convertible rows
    # Leave out rows that have year annotations such as quarters, future targets
    # E.g. `2020Q1`, `2050 Target`
    df = df[df["year"].apply(lambda x: str(x).isdigit() or isinstance(x, (float, int)))]
    df["year"] = df["year"].astype(int)

    # Filter rows based on alpha_3_code presence in iso_3_map
    df = df.loc[df["alpha_3_code"].isin(iso_3_map.values())].reset_index(drop=True)

    # Ensure canonical columns and sort them
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)

    assert df["value"].notna().any(), "All values are null"

    # Assert no duplicate rows exist after transformation
    assert (
        df.drop("value", axis=1).duplicated().sum() == 0
    ), exceptions.DUPLICATE_ERROR_MESSAGE

    return df
