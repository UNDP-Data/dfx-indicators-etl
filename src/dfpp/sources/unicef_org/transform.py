"""scripts to transform series data retrieved via api into publishable format"""

import pandas as pd

from dfpp.transformation.column_name_template import (
    ensure_canonical_columns,
    sort_columns_canonically,
)

from ...utils import get_country_metadata
from ...validation import check_duplicates


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
    COLUMNS = {
        "REF_AREA": "alpha_3_code",
        "series_id": "series_id",
        "Indicator": "series_name",
        "Sex": "disagr_sex",
        "Current age": "disagr_age",
        "TIME_PERIOD": "year",
        "OBS_VALUE": "value",
        "Unit of measure": "prop_unit",
    }
    df = df.reindex(columns=COLUMNS).rename(columns=COLUMNS)
    df = df.loc[df["alpha_3_code"].isin(get_country_metadata("iso-alpha-3"))].copy()
    df["year"] = df["year"].astype(int)
    df["source"] = "https://sdmx.data.unicef.org"
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    check_duplicates(df)
    return df.reset_index(drop=True)
