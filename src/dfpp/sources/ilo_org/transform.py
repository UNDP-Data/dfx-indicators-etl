"""
Transform raw data from the ILO.
"""

import pandas as pd

from ...transformation.column_name_template import (
    ensure_canonical_columns,
    sort_columns_canonically,
)
from ...validation import check_duplicates
from .retrieve import DISAGGREGATIONS, get_codelist_mapping

__all__ = ["transform"]


COLUMNS = {
    "REF_AREA": "alpha_3_code",
    "series_code": "series_id",
    "SEX": "disagr_sex",
    "AGE": "disagr_age",
    "GEO": "disagr_geo",
    "EDU": "disagr_edu",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "value",
    "OBS_STATUS": "prop_observation_type",
    "UNIT_MEASURE_TYPE": "prop_unit",
}


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tranform function for raw ILO data.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data as returned by the retrieval section.

    Returns
    -------
    pd.DataFrame
        Standardised data frame.
    """
    # Subset annual indicators
    df = df.query("FREQ == 'A'").copy()

    # Keep only aggregate to avoid overlaps between aggregate, 5- and 10-year bands
    # and different classifications for education too
    for column in ("AGE", "EDU"):
        if column in df.columns:
            df = df.loc[df[column].str.contains("AGGREGATE", na=True)].copy()

    # Replace disaggregation codes with labels
    mapping = {
        disaggregation: get_codelist_mapping(disaggregation)
        for disaggregation in DISAGGREGATIONS
    }
    df = df.replace(mapping)
    # Remap measure types
    mapping = get_codelist_mapping("UNIT_MEASURE")
    df["UNIT_MEASURE_TYPE"] = df["UNIT_MEASURE_TYPE"].map(mapping)

    # Reindex and rename columns
    df = df.reindex(columns=COLUMNS).rename(columns=COLUMNS)

    # Add indicator name
    mapping = get_codelist_mapping("INDICATOR")
    df["series_name"] = df["series_id"].map(mapping)
    df["year"] = df["year"].astype(int)
    df["source"] = "https://ilostat.ilo.org"
    df = ensure_canonical_columns(df)
    df = sort_columns_canonically(df)
    check_duplicates(df)
    return df.reset_index(drop=True)
