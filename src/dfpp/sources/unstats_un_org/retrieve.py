"""
Functions to retrieve data from UN Stats SDG API.
See https://unstats.un.org/sdgs/UNSDGAPIV5/swagger/index.html.
"""

from urllib.parse import urljoin

import httpx
import pandas as pd
from tqdm import tqdm

__all__ = ["get_series_metadata", "get_series_data", "get_dimensions", "get_attributes"]

BASE_URL = "https://unstats.un.org/sdgapi/v1/sdg/"


def get_series_metadata() -> pd.DataFrame:
    """
    Get series metadata from the UN Stats SDG API.

    Returns
    -------
    pd.DataFrame
        Data with series metadata.
    """
    response = httpx.get(urljoin(BASE_URL, "series/list"))
    response.raise_for_status()
    return pd.DataFrame(response.json())


def get_dimensions(series_id: str) -> dict:
    """
    Get series dimensions from the UN Stats SDG API.

    Returns
    -------
    dict
        Dimensions dictionary.
    """
    response = httpx.get(urljoin(BASE_URL, f"series/{series_id}/dimensions"))
    response.raise_for_status()
    return response.json()


def get_attributes(series_id: str) -> dict:
    """
    Get series attributes from the UN Stats SDG API.

    Returns
    -------
    dict
        Attributes dictionary.
    """
    response = httpx.get(urljoin(BASE_URL, f"series/{series_id}/attributes"))
    response.raise_for_status()
    return response.json()


def __get_series_data(
    client: httpx.Client,
    series_id: str,
    page: int,
    per_page: int = 1000,
) -> tuple[int, pd.DataFrame]:
    """
    Get series data from a single page.
    """
    url = urljoin(BASE_URL, "Series/Data")
    params = {"seriesCode": series_id, "pageSize": per_page, "page": page}
    response = client.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    pages = data["totalPages"]
    df = pd.DataFrame(data["data"])
    return pages, df


def get_series_data(series_id: str) -> pd.DataFrame | None:
    """
    Get series data from the UN Stats SDG API.

    Parameters
    ----------
    series_id : str
        Series ID. See `get_series_metadata`.

    Returns
    -------
    pd.DataFrame or None
        Data frame with data as returned by the API or None of no data is present.
    """
    try:
        data = []
        with httpx.Client(timeout=30) as client:
            pages, df = __get_series_data(client, series_id, 1)
            data.append(df)
            for page in tqdm(range(2, pages + 1)):
                pages, df = __get_series_data(client, series_id, page)
                data.append(df)
        return pd.concat(data, axis=0, ignore_index=True)
    except pd.errors.EmptyDataError:
        return None
    except httpx.HTTPError as error:
        print(series_id, error)
        return None
