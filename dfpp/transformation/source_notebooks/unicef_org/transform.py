"""scripts to transform series data retrieved via api into publishable format"""

import pandas as pd
import logging

from dfpp.transformation.source_notebooks.unicef_org.retrieve import BASE_URL
from dfpp.transformation.column_name_template import (
    DIMENSION_COLUMN_PREFIX,
    DIMENSION_COLUMN_CODE_SUFFIX,
    DIMENSION_COLUMN_NAME_SUFFIX,
    SexEnum,
    sort_columns_canonically,
    ensure_canonical_columns,
    CANONICAL_COLUMN_NAMES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

SEX_REMAP = {
    "F": SexEnum.FEMALE.value,
    "M": SexEnum.MALE.value,
    "_N": SexEnum.NON_RESPONSE.value,
    "_O": SexEnum.OTHER.value,
    "_T": SexEnum.TOTAL.value,
    "_U": SexEnum.UNKNOWN.value,
    "_Z": SexEnum.NOT_APPLICABLE.value,
}

PRIMARY_COLUMNS_TO_RENAME = {"REF_AREA": "alpha_3_code", "INDICATOR": "series_id"}


def transform_indicator(
    df_indicator: pd.DataFrame,
    df_indicators: pd.DataFrame,
    df_dimensions: pd.DataFrame,
    df_attributes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transform the raw indicator data into a processed DataFrame.
    Args:
        df_indicator (pd.DataFrame): The data to be transformed.
        df_indicators (pd.DataFrame): A DataFrame with the indicator codebook.
        df_dimensions (pd.DataFrame): A DataFrame with the dimension codebook.
        df_attributes (pd.DataFrame): A DataFrame with the attribute codebook.
    Returns:
        pd.DataFrame: The transformed data.
    """
    indicator_code = df_indicator["series_id"] = df_indicator["INDICATOR"].values[0]
    assert (
        df_indicator["INDICATOR"].nunique() == 1
    ), "Cannot process more than one series at a time"
    assert df_indicator["value"].notna().any(), f"{indicator_code}: All values are None"
    df_indicator["source"] = BASE_URL
    series_name = df_indicators[df_indicators["indicator_code"] == indicator_code][
        "indicator_name"
    ].values[0]
    df_indicator["series_name"] = series_name

    dimension_codes = df_dimensions.dimension_code.unique()
    attribute_codes = df_attributes.attribute_code.unique()

    for dimension_code in dimension_codes:
        if dimension_code in df_indicator.columns:
            if dimension_code in {"INDICATOR", "REF_AREA"}:
                continue

            column_name = dimension_code.lower().replace(" ", "_")

            to_remap = dict(
                df_dimensions[df_dimensions.dimension_code == dimension_code][
                    ["dimension_value_code", "dimension_value_name"]
                ].values
            )
            if column_name == "sex":
                to_remap = SEX_REMAP
            df_indicator[
                DIMENSION_COLUMN_PREFIX + column_name + DIMENSION_COLUMN_CODE_SUFFIX
            ] = df_indicator[dimension_code]
            df_indicator[
                DIMENSION_COLUMN_PREFIX + column_name + DIMENSION_COLUMN_NAME_SUFFIX
            ] = df_indicator[dimension_code].replace(to_remap)

            # Check if all values in the dimension_code column are in to_remap
            unmapped_values = set(df_indicator[dimension_code]) - set(to_remap.keys())
            if unmapped_values:
                logging.warning(
                    f"Warning: {dimension_code} contains unmapped values: {unmapped_values}"
                )

    for attribute_code in attribute_codes:
        if attribute_code in df_indicator.columns:
            to_remap = dict(
                df_attributes[df_attributes.attribute_code == attribute_code][
                    ["attribute_value_code", "attribute_value_name"]
                ].values
            )
            column_name = attribute_code.lower().replace(" ", "_")
            if attribute_code == "UNIT_MEASURE":
                column_name = "unit"
            if attribute_code == "OBS_STATUS":
                column_name = "observation_type"

            df_indicator[
                DIMENSION_COLUMN_PREFIX + column_name + DIMENSION_COLUMN_CODE_SUFFIX
            ] = df_indicator[attribute_code]
            df_indicator[
                DIMENSION_COLUMN_PREFIX + column_name + DIMENSION_COLUMN_NAME_SUFFIX
            ] = df_indicator[attribute_code].replace(to_remap)

            # Check if all values in the attribute_code column are in to_remap
            unmapped_values = set(df_indicator[attribute_code]) - set(to_remap.keys())
            if unmapped_values:
                logging.warning(
                    f"Warning: {attribute_code} contains unmapped values: {unmapped_values}"
                )

    to_rename = PRIMARY_COLUMNS_TO_RENAME.copy()
    valid_year_column = False
    for year_column_candidate in ["TIME_PERIOD", "OBS_FOOTNOTE"]:
        if df_indicator[year_column_candidate].notna().any():
            try:
                df_indicator[year_column_candidate] = df_indicator[
                    year_column_candidate
                ].astype("Int64")
                if (
                    df_indicator[year_column_candidate]
                    .apply(lambda x: 1000 <= x <= 9999 if x else True)
                    .all()
                ):
                    to_rename.update({year_column_candidate: "year"})
                    valid_year_column = True
                    break
            except ValueError:
                print(f"Could not convert {year_column_candidate} to Int64")

    if not valid_year_column:
        raise ValueError(
            f"None of the year columns contain valid 4-digit year values for {indicator_code}"
        )

    df_indicator.rename(columns=to_rename, inplace=True)
    df_indicator = ensure_canonical_columns(df_indicator)

    columns_to_select = [
        col
        for col in df_indicator.columns.tolist()
        if col not in CANONICAL_COLUMN_NAMES and DIMENSION_COLUMN_PREFIX in col
    ]

    df_indicator = df_indicator[CANONICAL_COLUMN_NAMES + columns_to_select]
    df_indicator = sort_columns_canonically(df_indicator)
    assert df_indicator.drop("value", axis=1).duplicated().sum() == 0
    return df_indicator
