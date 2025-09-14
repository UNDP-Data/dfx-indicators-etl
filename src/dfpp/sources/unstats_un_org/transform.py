"""scripts to transform series data retrieved via api into publishable format"""

import pandas as pd

from dfpp.transformation.column_name_template import (
    ensure_canonical_columns,
    sort_columns_canonically,
)

from ...utils import to_snake_case, replace_country_metadata
from ...validation import check_duplicates


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform UN Stats SDG API data.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data frame.

    Returns
    -------
    pd.DataFrame
        Transformed data frame in the canonical format.
    """
    columns = {
        "series": "series_id",
        "seriesDescription": "series_name",
        "alpha_3_code": "alpha_3_code",
        "timePeriodStart": "year",
        "value": "value",
        "prop_units": "prop_unit",
        "prop_nature": "prop_observation_type",
    }
    df["alpha_3_code"] = replace_country_metadata(
        df["geoAreaCode"], "m49", "iso-alpha-3"
    )
    df.dropna(subset=["alpha_3_code"], ignore_index=True, inplace=True)
    for column, prefix in (("attributes", "prop"), ("dimensions", "disagr")):
        df = df.join(
            pd.DataFrame(df[column].tolist()).rename(
                lambda name: to_snake_case(name, prefix=prefix), axis=1
            )
        )
    df = df.rename(columns=columns)
    df["year"] = df["year"].astype(int)
    df["source"] = "https://unstats.un.org"
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    check_duplicates(df)
    return df.reset_index(drop=True)
