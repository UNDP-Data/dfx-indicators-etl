import pandas as pd

from dfpp.sources.imf_org.retrieve import BASE_URL
from dfpp.transformation import exceptions
from dfpp.transformation.column_name_template import (
    SERIES_PROPERTY_PREFIX,
    ensure_canonical_columns,
    sort_columns_canonically,
)
from dfpp.transformation.value_handler import handle_value

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
    df[SERIES_PROPERTY_PREFIX + "unit"] = indicator["unit"]

    df["series_id"] = indicator["id"]
    df["series_name"] = indicator["label"]

    df = filter_out_regions(df, iso_3_map)
    df[["value", SERIES_PROPERTY_PREFIX + "value_label"]] = df.apply(
        handle_value, axis=1, result_type="expand"
    )
    df = ensure_canonical_columns(df)
    df_final = sort_columns_canonically(df)
    assert (
        df_final.drop("value", axis=1).duplicated().sum() == 0
    ), exceptions.DUPLICATE_ERROR_MESSAGE
    return df_final
