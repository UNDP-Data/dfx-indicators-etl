"""
Functions to retrieve data from SIPRI Military Expenditure Database.
See https://www.sipri.org/databases/milex.
"""

import pandas as pd

__all__ = ["get_series_metadata", "get_series_data"]

# BASE_URL = "https://www.sipri.org/sites/default/files/SIPRI-Milex-data-1948-2023.xlsx"
BASE_URL = "https://www.sipri.org/sites/default/files/SIPRI-Milex-data-1949-2024_2.xlsx"
DF_METADATA = pd.DataFrame(
    [
        (
            "Current US$",
            "SIPRI_MILEXT_CURRENT_USD",
            "Military expenditure by country in current US$ m., presented according to calendar year.",
            "Million USD (current)",
        ),
        (
            "Share of GDP",
            "SIPRI_MILEXT_SHARE_OF_GDP",
            "Military expenditure by country as a share of gross domestic product (GDP), presented according to calendar year.",
            "Share of GDP",
        ),
        (
            "Per capita",
            "SIPRI_MILEXT_PER_CAPITA",
            "Military expenditure per capita, in current US$, presented according to calendar year (1988-2024 only).",
            "USD (current)",
        ),
        (
            "Share of Govt. spending",
            "SIPRI_MILEXT_SHARE_OF_GOV_SPENDING",
            "Military expenditure as a percentage of general government expenditure (1988-2024 only).",
            "Percent of Government Spending",
        ),
    ],
    columns=["sheet_name", "series_id", "series_name", "prop_unit"],
)


def get_series_metadata() -> pd.DataFrame:
    """
    Get series metadata from the SIPRI Military Expenditure Database.

    Returns
    -------
    pd.DataFrame
        Data frame with metadata columns.
    """
    xlsx = pd.ExcelFile(BASE_URL)
    return DF_METADATA.loc[DF_METADATA["sheet_name"].isin(xlsx.sheet_names)].copy()


def get_series_data(series_id: str) -> pd.DataFrame:
    """
    Get series data from the the SIPRI Military Expenditure Database.

    Parameters
    ----------
    series_id : str
        Series ID. See `get_series_metadata`.

    Returns
    -------
    pd.DataFrame or None
        Data frame with country data in the wide format.
    """
    mapping = dict(DF_METADATA[["series_id", "sheet_name"]].values)
    if (sheet_name := mapping[series_id]) is None:
        return None
    # Infer the header row
    df = pd.read_excel(BASE_URL, sheet_name=sheet_name)
    header = df.iloc[:, 0].eq("Country").idxmax() + 1
    return pd.read_excel(
        BASE_URL, sheet_name=sheet_name, header=header, na_values=["xxx", "..."]
    )
