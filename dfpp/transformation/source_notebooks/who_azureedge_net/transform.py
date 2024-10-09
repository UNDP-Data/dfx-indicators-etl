import pandas as pd
import re

from dfpp.transformation.column_name_template import SexEnum, sort_columns_canonically

SOURCE_NAME = "WHO_GHO_API"


TO_RENAME_BASE_COLUMNS: dict[str, str] = {
    "TimeDim": "year",
    "NumericValue": "value",
    "SpatialDim": "alpha_3_code",
}

RECODE_SEX: dict[str, str] = {
    "SEX_MLE": SexEnum.MALE.value,
    "SEX_BTSX": SexEnum.BOTH.value,
    "SEX_FMLE": SexEnum.FEMALE.value,
    "SEX_NOA": SexEnum.NOT_APPLICABLE.value,
}


def sanitize_category(s):
    """Sanitize column names."""
    s = s.split(":")[0]
    s = s.lower()
    s = re.sub(r"[()\[\]]", "", s)
    s = re.sub(r"[\s\W]+", "_", s)
    s = s.strip("_")
    return s


def process_dimension_data(df_full_dimension_map):
    """Process raw dimension merged dimension map consisting of
    dimensions and their titles (list of possible values in human readable form) and sanitize columns.
    """
    df_full_dimension_map.columns = [
        sanitize_category(s) for s in df_full_dimension_map.columns
    ]
    df_full_dimension_map["dimension_to_display"] = df_full_dimension_map[
        "title_dimension"
    ].apply(sanitize_category)
    return df_full_dimension_map


def update_dimensional_columns(
    df: pd.DataFrame,
    df_full_dimension_map: pd.DataFrame,
    to_rename: dict[str, str],
    ISO_3_MAP: dict[str, str],
) -> pd.DataFrame:
    """Update dimensional column names and their values based on the full dimension mapping."""
    for i in range(1, 4):
        dim_column = f"Dim{i}"
        dim_type_column = f"Dim{i}Type"

        if df[dim_type_column].isna().all() or df[dim_column].isna().all():
            continue

        dim_type: list[str] = df[dim_type_column].dropna().unique()

        assert len(dim_type) == 1, "More than one dimension type per column"

        dimension: pd.DataFrame = df_full_dimension_map[
            df_full_dimension_map.code_dimension == dim_type[0]
        ]
        dimension_name_to_display: str = dimension["dimension_to_display"].values[0]

        to_rename.update({dim_column: dimension_name_to_display})

        if dimension_name_to_display == "sex":
            df[dim_column].replace(RECODE_SEX, inplace=True)
        else:
            to_replace: dict[str, str] = dict(
                dimension[["code_value", "title_value"]].values
            )

            df[dim_column].replace(to_replace, inplace=True)

    df.rename(columns=to_rename, inplace=True)
    df["country_or_area"] = df["alpha_3_code"].replace(ISO_3_MAP)
    return df


def handle_value_column(df: pd.DataFrame, to_rename: dict[str, str]) -> dict[str, str]:
    """Adjust renaming map if NumericValue is missing."""
    if df["NumericValue"].isna().all():
        to_rename.pop("NumericValue", None)
        to_rename.update({"Value": "value"})
    return to_rename


def filter_by_country_and_year(df: pd.DataFrame) -> pd.DataFrame:
    """Filter the DataFrame to include only rows where SpatialDimType is COUNTRY and TimeDimType is YEAR."""
    return df[
        (df.SpatialDimType == "COUNTRY") & (df.TimeDimType == "YEAR")
    ].reset_index()


def transform_indicator(
    series_id: str,
    df: pd.DataFrame,
    df_full_dimension_map: pd.DataFrame,
    ISO_3_MAP: dict[str, str],
) -> pd.DataFrame:
    """Transform the raw indicator data into a processed DataFrame."""
    assert not df.empty, "No data to transform"

    df: pd.DataFrame = filter_by_country_and_year(df)

    if df.empty:
        return None

    to_rename_columns: dict[str, str] = TO_RENAME_BASE_COLUMNS.copy()

    to_rename_columns = handle_value_column(df, to_rename_columns)

    df = update_dimensional_columns(
        df, df_full_dimension_map, to_rename_columns, ISO_3_MAP=ISO_3_MAP
    )

    df = df[["country_or_area"] + list(to_rename_columns.values())]

    assert df["value"].notna().any(), "All values are null"

    df["series_id"] = series_id
    df["source"] = SOURCE_NAME

    df = sort_columns_canonically(df)

    return df
