"""transform series data retrieved via api into publishable format"""

from typing import Dict, Optional, Tuple

import pandas as pd

from dfpp.sources.ilo_org.retrieve import BASE_URL
from dfpp.sources.ilo_org.utils import extract_last_braket_string, sanitize_category
from dfpp.transformation.column_name_template import (
    DIMENSION_COLUMN_PREFIX,
    SERIES_PROPERTY_PREFIX,
    SexEnum,
    sort_columns_canonically,
    ensure_canonical_columns,
)
from dfpp.transformation.value_handler import handle_value
from dfpp.sources import exceptions

SOURCE_NAME = "ILO_RPLUMBER_API"

__all__ = ["sanitize_categories", "transform_indicator"]


PRIMARY_COLUMNS_TO_RENAME = {
    "ref_area": "alpha_3_code",
    "time": "year",
    "obs_value": "value",
    "obs_status": "observation_type",
}


REMAP_SEX = {
    "SEX_T": SexEnum.TOTAL.value,
    "SEX_M": SexEnum.MALE.value,
    "SEX_F": SexEnum.FEMALE.value,
    "SEX_O": SexEnum.OTHER.value,
}


TO_DROP_COLUMN_NAME_PREFIXES = ("note_", "source", "indicator")


def sanitize_categories(df_classif1: pd.DataFrame, df_classif2: pd.DataFrame):
    """return sanitized category map used to rename columns and the values in classif1 and classif2 to human readable format"""
    df_classif1["dimension"] = df_classif1["classif1"].str.split("_").str[0].str.lower()
    df_classif1["category"] = df_classif1["classif1_label"].apply(sanitize_category)
    df_classif1["value"] = df_classif1["classif1_label"].str.split(": ").str[-1]

    df_classif2["dimension"] = df_classif2["classif2"].str.split("_").str[0].str.lower()
    df_classif2["category"] = df_classif2["classif2_label"].apply(sanitize_category)
    df_classif2["value"] = df_classif2["classif2_label"].str.split(": ").str[-1]
    return df_classif1, df_classif2


def replace_sex_values(df: pd.DataFrame, remap_sex: dict[str, str]) -> pd.DataFrame:
    """
    Replace the values in the 'sex' column with the remapped values.
    """
    if "sex" in df.columns:
        df[f"{DIMENSION_COLUMN_PREFIX}sex"] = df[
            "sex"
        ].replace(remap_sex)
        df.drop(columns=["sex"], inplace=True)
    return df


def rename_dimension_columns(
    df: pd.DataFrame,
    df_classif1: pd.DataFrame,
    df_classif2: pd.DataFrame,
) -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    """
    Rename the columns in df based on the dimension map in df_classif1 and df_classif2
    (human readable names of dimension collumns calssif1 and classif2)

    Args:
        df (pd.DataFrame): The source DataFrame with the columns to rename.
        df_classif1 (pd.DataFrame): The first classification DataFrame codebook (for classif1 column).
        df_classif2 (pd.DataFrame): The second classification DataFrame codebook (for classif2 column).

    Returns:
        Tuple[pd.DataFrame, Optional[str], Optional[str]]: A tuple containing the
            modified df, the name of the first dimension, and the name of the
            second dimension.
    """
    to_rename_columns = PRIMARY_COLUMNS_TO_RENAME.copy()

    dimension_one, dimension_two = None, None

    if "classif1" in df.columns:
        dimension_one = df_classif1[
            df_classif1.classif1 == df.classif1.iloc[0]
        ].category.values[0]
        to_rename_columns.update({"classif1": dimension_one})

    if "classif2" in df.columns:
        dimension_two = df_classif2[
            df_classif2.classif2 == df.classif2.iloc[0]
        ].category.values[0]
        to_rename_columns.update({"classif2": dimension_two})

    df.rename(columns=to_rename_columns, inplace=True)

    return df, dimension_one, dimension_two


def replace_dimension_values(
    df: pd.DataFrame,
    df_classif1: pd.DataFrame,
    df_classif2: pd.DataFrame,
    dimension_one: Optional[str],
    dimension_two: Optional[str],
) -> pd.DataFrame:
    """
    Replace the values of the columns in df with the values in df_classif1 and df_classif2
    (after human readable names of dimension collumns calssif1 and classif2 are set, this function
    sets the human readable values)

    Args:
        df (pd.DataFrame): The source DataFrame with the columns to rename.
        df_classif1 (pd.DataFrame): The first classification DataFrame codebook (for classif1 column).
        df_classif2 (pd.DataFrame): The second classification DataFrame codebook (for classif2 column).
        dimension_one (Optional[str]): The name of the first dimension after renaming the column classif1.
        dimension_two (Optional[str]): The name of the second dimension after renaming the column classif2.

    Returns:
        pd.DataFrame: The modified df with the replaced values.
    """

    if dimension_one and dimension_one in df.columns:
        dimension_one_map = dict(
            df_classif1[
                ["classif1", "classif1_label"]
            ].values
        )
        df[
            f"{DIMENSION_COLUMN_PREFIX}{dimension_one}"
        ] = df[dimension_one].replace(dimension_one_map)

        df.drop(columns=[dimension_one], inplace=True)

    if dimension_two and dimension_two in df.columns:
        dimension_two_map = dict(
            df_classif2[
                ["classif2", "classif2_label"]
            ].values
        )
        df[
            f"{DIMENSION_COLUMN_PREFIX}{dimension_two}"
        ] = df[dimension_two].replace(dimension_two_map)
        df.drop(columns=[dimension_two], inplace=True)
    return df


def filter_out_regions(df: pd.DataFrame, iso_3_map: dict) -> pd.DataFrame:
    """
    Filter out regions (rows containing 'X' followed by digits) from the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to filter.

    Returns:
        pd.DataFrame: A DataFrame with regions removed.
    """
    return df.loc[df["alpha_3_code"].isin(iso_3_map.keys())].reset_index(drop=True)


def transform_indicator(
    indicator: dict[str, str],
    df: pd.DataFrame,
    data_codes: dict[str, pd.DataFrame],
    iso_3_map: Dict[str, str],
) -> pd.DataFrame:
    """
    Process the ILO dataframe by replacing the 'sex' column values, renaming the classification columns,
    replacing the values of the classification columns with the values from df_classif1 and df_classif2,
    replacing the values of the 'country_or_area' column with the ISO 3 country codes, and filtering out regions.

    Args:
        indicator (dict[str, str]) indicator dict with its metadata
        df (pd.DataFrame): The source DataFrame.
        data_codes (dict[str, pd.DataFrame]): A dictionary of data codebooks.
        iso_3_map (Dict[str, str]): A dictionary mapping the ISO3 country codes to their human readable names.

    Returns:
        pd.DataFrame: A DataFrame with the processed data.
    """
    columns_to_drop = [
        column
        for column in df.columns
        if column.startswith(TO_DROP_COLUMN_NAME_PREFIXES)
    ]

    df.drop(columns=columns_to_drop, inplace=True)

    df = replace_sex_values(df, REMAP_SEX)

    df, dimension_one, dimension_two = rename_dimension_columns(
        df,
        data_codes["classif1"],
        data_codes["classif2"],
    )

    df = replace_dimension_values(
        df, data_codes["classif1"], data_codes["classif2"], dimension_one, dimension_two
    )

    df = filter_out_regions(df, iso_3_map)

    if "observation_type" in df.columns:
        observation_type_map = dict(
            data_codes["obs_status"][["obs_status", "obs_status_label"]].values
        )
        df[
            SERIES_PROPERTY_PREFIX + "observation_type"
        ] = df["observation_type"].replace(observation_type_map)
        df.drop(columns=["observation_type"], inplace=True)
    df["series_id"] = indicator["id"]
    df["series_name"] = indicator["indicator_label"]
    df[SERIES_PROPERTY_PREFIX + "unit"] = (
        extract_last_braket_string(df["series_name"].values[0])
    )
    df["source"] = BASE_URL
    df[["value", SERIES_PROPERTY_PREFIX + "value_label"]] = df.apply(handle_value, axis=1, result_type="expand")
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    assert (
        df.drop("value", axis=1).duplicated().sum() == 0
    ), exceptions.DUPLICATE_ERROR_MESSAGE
    return df
