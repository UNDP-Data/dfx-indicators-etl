"""
Functions to retrieve data from WHO GHO API.
See https://www.who.int/data/gho/info/gho-odata-api.
"""

from urllib.parse import urljoin

import httpx
import pandas as pd

BASE_URL = "https://ghoapi.azureedge.net/api/"

__all__ = ["get_series_metadata", "get_dimensions", "get_series_data"]


def get_series_metadata() -> pd.DataFrame:
    """
    Get series metadata from the WHO GHO API.

    Returns
    -------
    pd.DataFrame
        Data with series metadata.
    """
    response = httpx.get(urljoin(BASE_URL, "Indicator"))
    response.raise_for_status()
    return pd.DataFrame(response.json()["value"])


def get_dimensions() -> dict:
    """
    Get series dimensions from the WHO GHO API.

    Returns
    -------
    dict
        Dimensions dictionary.
    """
    response = httpx.get(urljoin(BASE_URL, "DIMENSION"))
    response.raise_for_status()
    return response.json()["value"]


def get_series_data(series_id: str, **kwargs) -> pd.DataFrame | None:
    """
    Get series data from the WHO GHO API.

    Parameters
    ----------
    series_id : str
        Series ID. See `get_series_metadata`.
    **kwargs
        Keywords arguments used as filters for the data.

    Returns
    -------
    pd.DataFrame or None
        Data frame with data as returned by the API or None of no data is present.
    """
    filters = ["NumericValue ne null"]
    for k, v in kwargs.items():
        if isinstance(v, (str, int)):
            filters.append(f"{k} eq '{v}'")
        elif isinstance(v, list):
            filters.append(f"{k} in {tuple(v)}")
        else:
            raise ValueError(f"{k} must be one of (str, int, list). Found {type(v)}")
    filters = f"?$filter={' and '.join(filters)}" if filters else ""
    try:
        with httpx.Client(timeout=30) as client:
            response = client.get(urljoin(BASE_URL, f"{series_id}{filters}"))
            response.raise_for_status()
        return pd.DataFrame(response.json()["value"])
    except pd.errors.EmptyDataError:
        return None
    except httpx.HTTPError as error:
        print(series_id, error)
        return None
