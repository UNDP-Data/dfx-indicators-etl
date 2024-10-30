"""scripts to transform series data retrieved via api into publishable format"""

import pandas as pd

from dfpp.transformation.column_name_template import (
    DIMENSION_COLUMN_PREFIX,
    DIMENSION_COLUMN_CODE_SUFFIX,
    DIMENSION_COLUMN_NAME_SUFFIX,
    SexEnum,
    sort_columns_canonically,
    CANONICAL_COLUMN_NAMES,
)

from dfpp.transformation.source_notebooks.un_org.retrieve import BASE_URL

SEX_REMAP = {
    "BOTHSEX": SexEnum.BOTH.value,
    "MALE": SexEnum.MALE.value,
    "FEMALE": SexEnum.FEMALE.value,
}

PRIMARY_COLUMNS_TO_RENAME = {
    "geoAreaCode": "alpha_3_code",
    "timePeriodStart": "year",
    "seriesDescription": "series_name",
}


__all__ = ["transform_series"]


def transform_series(
    series_id: str,
    df: pd.DataFrame,
    df_dimension_codebook: pd.DataFrame = None,
    df_attribute_codebook: pd.DataFrame = None,
    iso_3_map_numeric_to_alpha: dict = None,
) -> pd.DataFrame:
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
            df[
                f"{DIMENSION_COLUMN_PREFIX}{column_name_formatted}{DIMENSION_COLUMN_CODE_SUFFIX}"
            ] = df[column]
            df[
                f"{DIMENSION_COLUMN_PREFIX}{column_name_formatted}{DIMENSION_COLUMN_NAME_SUFFIX}"
            ] = df[column].replace(SEX_REMAP)
            continue

        df[
            f"{DIMENSION_COLUMN_PREFIX}{column_name_formatted}{DIMENSION_COLUMN_CODE_SUFFIX}"
        ] = df[column]
        df[
            f"{DIMENSION_COLUMN_PREFIX}{column_name_formatted}{DIMENSION_COLUMN_NAME_SUFFIX}"
        ] = df[column].replace(to_remap)

    for column in df_attribute_codebook["id"].unique().tolist():
        if not column in df.columns:
            continue
        to_remap = dict(
            df_attribute_codebook[df_attribute_codebook["id"] == column][
                ["code", "description"]
            ].values
        )
        if column == "Units":
            df[DIMENSION_COLUMN_PREFIX + "unit" + DIMENSION_COLUMN_CODE_SUFFIX] = df[
                column
            ]
            df[DIMENSION_COLUMN_PREFIX + "unit" + DIMENSION_COLUMN_NAME_SUFFIX] = df[
                column
            ].replace(to_remap)
            continue

        if column == "Nature":
            df[
                DIMENSION_COLUMN_PREFIX
                + "observation_type"
                + DIMENSION_COLUMN_CODE_SUFFIX
            ] = df[column]
            df[
                DIMENSION_COLUMN_PREFIX
                + "observation_type"
                + DIMENSION_COLUMN_NAME_SUFFIX
            ] = df[column].replace(to_remap)
            continue

        column_name_formatted = column.lower().replace(" ", "_")
        df[
            f"{DIMENSION_COLUMN_PREFIX}{column_name_formatted}{DIMENSION_COLUMN_CODE_SUFFIX}"
        ] = df[column]
        df[
            f"{DIMENSION_COLUMN_PREFIX}{column_name_formatted}{DIMENSION_COLUMN_NAME_SUFFIX}"
        ] = df[column].replace(to_remap)

    columns_to_rename = PRIMARY_COLUMNS_TO_RENAME.copy()

    df.rename(columns=columns_to_rename, inplace=True)

    disagr_columns = [
        col
        for col in df.columns
        if col.startswith(DIMENSION_COLUMN_PREFIX) and col not in CANONICAL_COLUMN_NAMES
    ]
    df["series_id"] = series_id
    df["source"] = BASE_URL
    df = df[CANONICAL_COLUMN_NAMES + disagr_columns]
    df["alpha_3_code"] = df["alpha_3_code"].astype(int)
    df["alpha_3_code"] = df["alpha_3_code"].replace(iso_3_map_numeric_to_alpha)
    df = df[df["alpha_3_code"].apply(lambda x: isinstance(x, str))].reset_index(
        drop=True
    )
    df = sort_columns_canonically(df)
    assert df.drop("value", axis=1).duplicated().sum() == 0
    df["value"] = df["value"].replace({"NaN": None})
    return df
