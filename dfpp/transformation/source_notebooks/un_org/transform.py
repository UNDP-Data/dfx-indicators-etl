"""scripts to transform series data retrieved via api into publishable format"""

import pandas as pd

from dfpp.transformation.column_name_template import DIMENSION_COLUMN_PREFIX, SexEnum, sort_columns_canonically

from dfpp.transformation.source_notebooks.un_org.retrieve import BASE_URL

SEX_REMAP = {
    "BOTHSEX": SexEnum.BOTH.value,
    "MALE": SexEnum.MALE.value,
    "FEMALE": SexEnum.FEMALE.value,
}

AGE_REMAP = {"ALLAGE", "all"}

PRIMARY_COLUMNS_TO_RENAME = {
    "geoAreaCode": "alpha_3_code",
    "timePeriodStart": "year",
    "seriesDescription": "series_name",
    "Units": "unit",
    "Nature": "observation_type",
}


__all__ = ["transform_series"]


def transform_series(
    series_id: str,
    df: pd.DataFrame,
    df_dimension_codebook: pd.DataFrame = None,
    df_attribute_codebook: pd.DataFrame = None,
    iso_3_map_numeric_to_alpha: dict = None,
) -> pd.DataFrame:
    """transform a signle series dataframe to the publishable format
    Args:
        series_id (str): the series id
        df (pd.DataFrame): the dataframe to transform
        df_dimension_codebook (pd.DataFrame): the dimension column codebook
        df_attribute_codebook (pd.DataFrame): the attributes codebook
        iso_3_map_numeric_to_alpha (dict): the iso_3_map_numeric_to_alpha to remap numeric country names to alpha iso3
    Returns:
        pd.DataFrame: the transformed dataframe
    """
    df_dimensions = pd.json_normalize(df["dimensions"])
    df_attributes = pd.json_normalize(df["attributes"])
    
    df = pd.concat([df.drop(columns=["dimensions", "attributes"]), df_dimensions, df_attributes], axis=1)

    dimension_columns = df_dimension_codebook["id"].unique().tolist()
    dimension_columns_formatted = [column.lower().replace(" ", "_") for column in dimension_columns]

    for column in dimension_columns:
        if column in df.columns:
            to_remap = dict(df_dimension_codebook[df_dimension_codebook["id"] == column][["code", "description"]].values)
            df[column] = df[column].replace(to_remap)
    
    for column in df_attribute_codebook["id"].unique().tolist():
        if column in df.columns:
            to_remap = dict(df_attribute_codebook[df_attribute_codebook["id"] == column][["code", "description"]].values)
            df[column] = df[column].replace(to_remap)

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
        ["series_name", "alpha_3_code", "year", "value", "unit", "observation_type"]
        + dimension_columns_formatted
    ]
    df_selection["alpha_3_code"] = df_selection["alpha_3_code"].astype(int)
    df_selection["alpha_3_code"] = df_selection["alpha_3_code"].replace(iso_3_map_numeric_to_alpha)

    if "age" in df.columns:
        df_selection["age"] = df_selection["age"].replace(AGE_REMAP)

    if "sex" in df.columns:
        df_selection["sex"] = df_selection["sex"].replace(SEX_REMAP)

    df_selection.columns = [
        DIMENSION_COLUMN_PREFIX + col if col in dimension_columns_formatted else col
        for col in df_selection.columns
    ]

    df_selection["source"] = BASE_URL
    df_selection["series_id"] = series_id

    df_selection = sort_columns_canonically(df_selection)
    assert df_selection.drop("value", axis=1).duplicated().sum() == 0, "Duplicate rows per country year found after transformation, make sure that any dimension columns are not omitted from the transformed data."
    return df_selection
