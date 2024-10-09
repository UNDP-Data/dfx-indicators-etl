"""scripts to transform series data retrieved via api into publishable format"""

import pandas as pd

from dfpp.transformation.column_name_template import SexEnum, sort_columns_canonically

SOURCE_NAME = "UN_ORG_API"

SEX_REMAP = {
    "BOTHSEX": SexEnum.BOTH.value,
    "MALE": SexEnum.MALE.value,
    "FEMALE": SexEnum.FEMALE.value,
}

AGE_REMAP = {"ALLAGE", "all"}

PRIMARY_COLUMNS_TO_RENAME = {
    "geoAreaCode": "alpha_3_code",
    "geoAreaName": "country_or_area",
    "timePeriodStart": "year",
}


__all__ = ["transform_series"]


def transform_series(
    series_id: str,
    df: pd.DataFrame,
    dimension_columns: list[str] = None,
    iso_3_map_numeric_to_alpha: dict = None,
    iso_3_map_alpha_to_official: dict = None,
) -> pd.DataFrame:
    """transform a signle series dataframe to the publishable format
    Args:
        series_id (str): the series id
        df (pd.DataFrame): the dataframe to transform
        dimension_columns (list[str]): the dimension columns
        iso_3_map_numeric_to_alpha (dict): the iso_3_map_numeric_to_alpha to remap numeric country names to alpha iso3
    Returns:
        pd.DataFrame: the transformed dataframe
    """
    df_dimensions = pd.json_normalize(df["dimensions"])

    df = pd.concat([df.drop(columns=["dimensions"]), df_dimensions], axis=1)

    dimension_columns_formatted = [
        column.lower().replace(" ", "_") for column in dimension_columns
    ]
    dimension_column_rename_map = dict(
        zip(dimension_columns, dimension_columns_formatted)
    )

    columns_to_rename = PRIMARY_COLUMNS_TO_RENAME.copy()
    columns_to_rename.update(dimension_column_rename_map)

    df.rename(
        columns=columns_to_rename,
        inplace=True,
    )

    df_selection = df.copy()[
        ["alpha_3_code", "country_or_area", "year", "value"]
        + dimension_columns_formatted
    ]
    df_selection["alpha_3_code"] = df_selection["alpha_3_code"].astype(int)
    df_selection["alpha_3_code"] = df_selection["alpha_3_code"].replace(iso_3_map_numeric_to_alpha)
    df_selection["country_or_area"] = df_selection["alpha_3_code"].replace(iso_3_map_alpha_to_official)

    if "age" in df.columns:
        df_selection["age"] = df_selection["age"].replace(AGE_REMAP)

    if "sex" in df.columns:
        df_selection["sex"] = df_selection["sex"].replace(SEX_REMAP)

    df_selection["source"] = SOURCE_NAME
    df_selection["series_id"] = series_id

    df_selection = sort_columns_canonically(df_selection)
    return df_selection
