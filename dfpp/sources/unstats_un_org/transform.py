"""scripts to transform series data retrieved via api into publishable format"""

import pandas as pd

from dfpp.sources.unstats_un_org.retrieve import BASE_URL
from dfpp.transformation.column_name_template import (
    CANONICAL_COLUMN_NAMES,
    DIMENSION_COLUMN_PREFIX,
    SERIES_PROPERTY_PREFIX,
    SexEnum,
    sort_columns_canonically,
    ensure_canonical_columns,
)
from dfpp.transformation.value_handler import handle_value
from dfpp.transformation import exceptions

SEX_REMAP = {
    "BOTHSEX": SexEnum.BOTH.value,
    "MALE": SexEnum.MALE.value,
    "FEMALE": SexEnum.FEMALE.value,
}

PRIMARY_COLUMNS_TO_RENAME = {
    "geoAreaCode": "alpha_3_code",
    "timePeriodStart": "year",
    "seriesDescription": "series_name",
    "indicator": DIMENSION_COLUMN_PREFIX + "sdg_indicator",
}


__all__ = ["transform_series"]


def transform_series(
    series_id: str,
    df: pd.DataFrame,
    df_dimension_codebook: pd.DataFrame = None,
    df_attribute_codebook: pd.DataFrame = None,
    iso_3_map_numeric_to_alpha: dict = None,
) -> pd.DataFrame:
    """
    Transform a DataFrame containing series data retrieved via the UN API into a publishable format.

    Args:
        series_id (str): The ID of the series to be transformed.
        df (pd.DataFrame): The DataFrame containing the series data.
        df_dimension_codebook (pd.DataFrame, optional): The DataFrame containing the dimension codebook. Defaults to None.
        df_attribute_codebook (pd.DataFrame, optional): The DataFrame containing the attribute codebook. Defaults to None.
        iso_3_map_numeric_to_alpha (dict, optional): A dictionary mapping numeric to alpha 3 country codes. Defaults to None.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    df_dimensions = pd.json_normalize(df["dimensions"])
    df_attributes = pd.json_normalize(df["attributes"])

    df = pd.concat(
        [df.drop(columns=["dimensions", "attributes"]), df_dimensions, df_attributes],
        axis=1,
    )

    dimension_columns = df_dimension_codebook["id"].unique().tolist()

    for column in dimension_columns:
        if not column in df.columns:
            continue

        to_remap = dict(
            df_dimension_codebook[df_dimension_codebook["id"] == column][
                ["code", "description"]
            ].values
        )
        column_name_formatted = column.lower().replace(" ", "_")

        if column_name_formatted == "sex":
            to_remap = SEX_REMAP

        df[f"{DIMENSION_COLUMN_PREFIX}{column_name_formatted}"] = df[column].replace(
            to_remap
        )

    series_property_column_map = {"Nature": "observation_type", "Units": "unit"}
    for column in df_attribute_codebook["id"].unique().tolist():
        if not column in df.columns:
            continue

        to_remap = dict(
            df_attribute_codebook[df_attribute_codebook["id"] == column][
                ["code", "description"]
            ].values
        )
        column_name_formatted = column.lower().replace(" ", "_")

        if column in series_property_column_map.keys():
            column_name_formatted = series_property_column_map[column]

        df[f"{SERIES_PROPERTY_PREFIX}{column_name_formatted}"] = df[column].replace(
            to_remap
        )

    columns_to_rename = PRIMARY_COLUMNS_TO_RENAME.copy()

    df.rename(columns=columns_to_rename, inplace=True)

    df[["value", SERIES_PROPERTY_PREFIX + "value_label"]] = df.apply(
        handle_value, axis=1, result_type="expand"
    )

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

    df["series_id"] = series_id
    df["source"] = BASE_URL
    df = df[CANONICAL_COLUMN_NAMES + to_select_columns]

    df = resolve_many_indicators_to_one_observation(df)
    df["alpha_3_code"] = df["alpha_3_code"].astype(int)
    df["alpha_3_code"] = df["alpha_3_code"].replace(iso_3_map_numeric_to_alpha)
    df = df[df["alpha_3_code"].apply(lambda x: isinstance(x, str))].reset_index(
        drop=True
    )
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    df[DIMENSION_COLUMN_PREFIX + "sdg_indicator"] = df[
        DIMENSION_COLUMN_PREFIX + "sdg_indicator"
    ].apply(tuple)
    assert (
        df.drop("value", axis=1).duplicated().sum() == 0
    ), exceptions.DUPLICATE_ERROR_MESSAGE
    df[DIMENSION_COLUMN_PREFIX + "sdg_indicator"] = df[
        DIMENSION_COLUMN_PREFIX + "sdg_indicator"
    ].apply(list)
    df["value"] = df["value"].replace({"NaN": None})
    return df


def resolve_many_indicators_to_one_observation(df: pd.DataFrame) -> pd.DataFrame:
    """some series have repeating rows linked to different SDG indicator goals, this function
    removes the duplicates and returns a dataframe with one observation and all SDG indicators linked to it
    """
    df_grouped = df.groupby(
        df.columns.difference([DIMENSION_COLUMN_PREFIX + "sdg_indicator"]).tolist(),
        dropna=False,
    )

    df_result = (
        df_grouped[DIMENSION_COLUMN_PREFIX + "sdg_indicator"]
        .apply(lambda x: list(set(item for sublist in x for item in sublist)))
        .reset_index()
    )
    return df_result
