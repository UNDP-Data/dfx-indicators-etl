"""scripts to transform series data retrieved via api into publishable format"""

import logging

import pandas as pd

from dfpp.transformation.column_name_template import (
    CANONICAL_COLUMN_NAMES,
    DIMENSION_COLUMN_CODE_SUFFIX,
    DIMENSION_COLUMN_NAME_SUFFIX,
    DIMENSION_COLUMN_PREFIX,
    SexEnum,
    ensure_canonical_columns,
    sort_columns_canonically,
)
from dfpp.transformation.sources.unicef_org.retrieve import BASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

__all__ = ["transform"]

SEX_REMAP = {
    "F": SexEnum.FEMALE.value,
    "M": SexEnum.MALE.value,
    "_N": SexEnum.NON_RESPONSE.value,
    "_O": SexEnum.OTHER.value,
    "_T": SexEnum.TOTAL.value,
    "_U": SexEnum.UNKNOWN.value,
    "_Z": SexEnum.NOT_APPLICABLE.value,
}

PRIMARY_COLUMNS_TO_RENAME = {"REF_AREA": "alpha_3_code"}


def get_series_name(df_indicators: pd.DataFrame, indicator_code: str) -> str:
    """Retrieve the series name for a given indicator code."""
    return df_indicators[df_indicators["indicator_code"] == indicator_code][
        "indicator_name"
    ].values[0]


def remap_dimension(
    df: pd.DataFrame, dimension_code: str, to_remap: dict, column_name: str
) -> pd.DataFrame:
    """Remap dimension values and create canonical columns for dimension codes and names."""
    df[f"{DIMENSION_COLUMN_PREFIX}{column_name}{DIMENSION_COLUMN_CODE_SUFFIX}"] = df[
        dimension_code
    ]
    df[f"{DIMENSION_COLUMN_PREFIX}{column_name}{DIMENSION_COLUMN_NAME_SUFFIX}"] = df[
        dimension_code
    ].replace(to_remap)
    unmapped_values = set(df[dimension_code]) - set(to_remap.keys())
    if unmapped_values:
        logging.warning(
            f"Warning: {dimension_code} contains unmapped values: {unmapped_values}"
        )
    return df


def remap_dimensions(df: pd.DataFrame, df_dimensions: pd.DataFrame) -> pd.DataFrame:
    """Process and remap each dimension in the DataFrame."""
    for dimension_code in df_dimensions.dimension_code.unique():
        if dimension_code in df.columns and dimension_code not in {
            "INDICATOR",
            "REF_AREA",
        }:
            column_name = dimension_code.lower().replace(" ", "_")
            to_remap = (
                SEX_REMAP
                if column_name == "sex"
                else dict(
                    df_dimensions[df_dimensions.dimension_code == dimension_code][
                        ["dimension_value_code", "dimension_value_name"]
                    ].values
                )
            )
            df = remap_dimension(df, dimension_code, to_remap, column_name)
    return df


def remap_attribute(
    df: pd.DataFrame, attribute_code: str, to_remap: dict, column_name: str
) -> pd.DataFrame:
    """Remap attribute values and create canonical columns for attribute codes and names."""
    df[f"{DIMENSION_COLUMN_PREFIX}{column_name}{DIMENSION_COLUMN_CODE_SUFFIX}"] = df[
        attribute_code
    ]
    df[f"{DIMENSION_COLUMN_PREFIX}{column_name}{DIMENSION_COLUMN_NAME_SUFFIX}"] = df[
        attribute_code
    ].replace(to_remap)
    unmapped_values = set(df[attribute_code]) - set(to_remap.keys())
    if unmapped_values:
        logging.warning(
            f"Warning: {attribute_code} contains unmapped values: {unmapped_values}"
        )
    return df


def remap_attributes(df: pd.DataFrame, df_attributes: pd.DataFrame) -> pd.DataFrame:
    """Process and remap each attribute in the DataFrame."""
    for attribute_code in df_attributes.attribute_code.unique():
        if attribute_code in df.columns:
            column_name = attribute_code.lower().replace(" ", "_")
            if attribute_code == "UNIT_MEASURE":
                column_name = "unit"
            elif attribute_code == "OBS_STATUS":
                column_name = "observation_type"
            to_remap = dict(
                df_attributes[df_attributes.attribute_code == attribute_code][
                    ["attribute_value_code", "attribute_value_name"]
                ].values
            )
            df = remap_attribute(df, attribute_code, to_remap, column_name)
    return df


def validate_and_rename_year_column(df: pd.DataFrame) -> pd.DataFrame:
    """Identify and rename the valid year column."""
    for year_column in ["TIME_PERIOD", "OBS_FOOTNOTE"]:
        if df[year_column].notna().any():
            try:
                df[year_column] = df[year_column].astype("Int64")
                if (
                    df[year_column]
                    .apply(lambda x: 1000 <= x <= 9999 if x else True)
                    .all()
                ):
                    df.rename(columns={year_column: "year"}, inplace=True)
                    return df
            except ValueError:
                logging.warning(f"Could not convert {year_column} to Int64")
    raise ValueError("No valid 4-digit year column found")


def transform(
    df_indicator: pd.DataFrame = None,
    df_indicators: pd.DataFrame = None,
    df_dimensions: pd.DataFrame = None,
    df_attributes: pd.DataFrame = None,
    iso_3_map=None,
) -> pd.DataFrame:
    """
    Transform the raw indicator data into a processed DataFrame.

    Args:
        df_indicator (pd.DataFrame): The data to be transformed.
        df_indicators (pd.DataFrame): A DataFrame with the indicator codebook.
        df_dimensions (pd.DataFrame): A DataFrame with the dimension codebook.
        df_attributes (pd.DataFrame): A DataFrame with the attribute codebook.

    Returns:
        pd.DataFrame: A DataFrame with the processed data.
    """
    indicator_code = df_indicator["INDICATOR"].values[0]
    df_indicator["series_id"] = indicator_code
    assert (
        df_indicator["INDICATOR"].nunique() == 1
    ), "Cannot process more than one series at a time"
    assert df_indicator["value"].notna().any(), f"{indicator_code}: All values are None"
    df_indicator["source"] = BASE_URL
    df_indicator["series_name"] = get_series_name(df_indicators, indicator_code)
    df_indicator = remap_dimensions(df_indicator, df_dimensions)
    df_indicator = remap_attributes(df_indicator, df_attributes)
    df_indicator = validate_and_rename_year_column(df_indicator)
    df_indicator.rename(columns=PRIMARY_COLUMNS_TO_RENAME, inplace=True)
    df_indicator = ensure_canonical_columns(df_indicator)

    columns_to_select = [
        col
        for col in df_indicator.columns.tolist()
        if col not in CANONICAL_COLUMN_NAMES and DIMENSION_COLUMN_PREFIX in col
    ]
    df_indicator = df_indicator[CANONICAL_COLUMN_NAMES + columns_to_select]
    df_indicator = sort_columns_canonically(df_indicator)
    if iso_3_map:
        df_indicator = df_indicator[
            df_indicator["alpha_3_code"].isin(iso_3_map.keys())
        ].reset_index(drop=True)
    assert df_indicator.drop("value", axis=1).duplicated().sum() == 0
    return df_indicator
