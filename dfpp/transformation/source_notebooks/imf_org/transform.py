import pandas as pd
from dfpp.transformation.source_notebooks.imf_org.retrieve import BASE_URL

from dfpp.transformation.column_name_template import (
    DIMENSION_COLUMN_PREFIX,
    DIMENSION_COLUMN_CODE_SUFFIX,
    DIMENSION_COLUMN_NAME_SUFFIX,
    sort_columns_canonically,
)

__all__ = ["transform"]


def filter_out_regions(df: pd.DataFrame, iso_3_map: dict) -> pd.DataFrame:
    """
    Filter out IMF regions that are not ISO3 countries

    Args:
        df (pd.DataFrame): The DataFrame to filter.

    Returns:
        pd.DataFrame: A DataFrame with regions removed.
    """
    return df.loc[df["alpha_3_code"].isin(iso_3_map.keys())].reset_index(drop=True)


def transform(df: pd.DataFrame, indicator: dict, iso_3_map: dict):
    df = df.reset_index().rename(columns={"index": "year"})
    df = df.melt(id_vars="year", value_name="value", var_name="alpha_3_code")
    df["source"] = BASE_URL
    df[DIMENSION_COLUMN_PREFIX + "unit" + DIMENSION_COLUMN_NAME_SUFFIX] = indicator["unit"]
    df[DIMENSION_COLUMN_PREFIX + "unit" + DIMENSION_COLUMN_CODE_SUFFIX] = None

    df["series_id"] = indicator["id"]
    df["series_name"] = indicator["label"]
    df[DIMENSION_COLUMN_PREFIX + "observation_type" + DIMENSION_COLUMN_NAME_SUFFIX] = None
    df[DIMENSION_COLUMN_PREFIX + "observation_type" + DIMENSION_COLUMN_CODE_SUFFIX] = None

    df = filter_out_regions(df, iso_3_map)

    df_final = sort_columns_canonically(df)
    assert df_final.drop("value", axis=1).duplicated().sum() == 0
    return df_final
