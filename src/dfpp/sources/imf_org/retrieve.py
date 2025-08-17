"""
Functions to retrieve data from IMF DataMapper API.
See https://www.imf.org/external/datamapper/api/help.
"""

from urllib.parse import urljoin

import httpx
import pandas as pd

BASE_URL = "https://www.imf.org/external/datamapper/api/v1/"

__all__ = ["get_series_metadata", "get_series_data"]


def get_series_metadata() -> pd.DataFrame:
    """
    Get series metadata from the IMF DataMapper API indicators endpoint.

    Returns
    -------
    pd.DataFrame
        Data frame with twthree columns: `series_id`, `series_name` and `prop_unit`.
    """
    response = httpx.get(urljoin(BASE_URL, "indicators"))
    response.raise_for_status()
    data = response.json()
    data = [
        {"series_id": series_id} | metadata
        for series_id, metadata in data["indicators"].items()
        if series_id
    ]
    columns = {"series_id": "series_id", "label": "series_name", "unit": "prop_unit"}
    df = pd.DataFrame(data).reindex(columns=columns).rename(columns=columns)
    df["series_name"] = df["series_name"].str.strip()
    return df


def get_series_data(series_id: str) -> pd.DataFrame:
    """
    Get series data from the IMF DataMapper API endpoint.

    Parameters
    ----------
    series_id : str
        Series ID. See `get_series_metadata`.

    Returns
    -------
    pd.DataFrame or None
        Data frame with panel data from the API. When no data is available,
        None is returned.
    """
    response = httpx.get(urljoin(BASE_URL, series_id))
    response.raise_for_status()
    data = response.json()
    if (values := data.get("values")) is None:
        return None
    dfs = []
    for country_code, records in values[series_id].items():
        df = pd.DataFrame(records.items(), columns=["year", "value"])
        df.insert(0, "alpha_3_code", country_code)
        dfs.append(df)
    return pd.concat(dfs, axis=0, ignore_index=True)
