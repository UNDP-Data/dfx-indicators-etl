"""
Transform raw data from energydata.info.
"""

import country_converter as coco
import pandas as pd

from ...transformation.column_name_template import (
    CANONICAL_COLUMN_NAMES,
    DIMENSION_COLUMN_PREFIX,
    SERIES_PROPERTY_PREFIX,
    ensure_canonical_columns,
    sort_columns_canonically,
)
from ...transformation.value_handler import handle_value

__all__ = ["transform"]


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform ELECCAP series https://energydata.info/dataset/installed-electricity-capacity-by-country-area-mw-by-country.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data frame.

    Returns
    -------
    pd.DataFrame
        Transformed data frame in the canonical format.
    """
    cc = coco.CountryConverter()
    df = df.copy()
    df.columns = [
        "country",
        DIMENSION_COLUMN_PREFIX + "energy_technology",
        DIMENSION_COLUMN_PREFIX + "grid_connection",
        "year",
        "value",
    ]
    df.ffill(inplace=True)

    df["alpha_3_code"] = cc.pandas_convert(df["country"], to="ISO3")
    df = df[df["alpha_3_code"] != "not found"].reset_index(drop=True)

    df["year"] = df["year"].astype(int)

    df[SERIES_PROPERTY_PREFIX + "unit"] = "Megawatt"
    df["value"] = df["value"].astype("float")

    df["source"] = "https://energydata.info/"
    df["series_id"] = "irena_eleccap"
    df["series_name"] = (
        "Installed electricity capacity by country/area (MW) by Country/area, Technology, Grid connection and Year"
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
    df[["value", SERIES_PROPERTY_PREFIX + "value_label"]] = df.apply(
        handle_value, axis=1, result_type="expand"
    )
    df = ensure_canonical_columns(df)
    df = df[CANONICAL_COLUMN_NAMES + to_select_columns]
    df = sort_columns_canonically(df)
    return df
