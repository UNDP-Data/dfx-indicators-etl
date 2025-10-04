"""
Functions to retrieve data from the World Bank Indicator API.
See https://datahelpdesk.worldbank.org/knowledgebase/topics/125589-developer-information.
"""

from urllib.parse import urljoin

import httpx
import pandas as pd
from tqdm import tqdm

BASE_URL = "https://api.worldbank.org/v2/"

__all__ = ["get_series_metadata", "get_series_data"]


def __get_series_metadata(
    client: httpx.Client, page: int, per_page: int
) -> list[dict, list[dict]]:
    """
    Get a single metadata page.
    """
    response = client.get(
        urljoin(BASE_URL, "indicator"),
        params={"format": "json", "page": page, "per_page": per_page},
    )
    response.raise_for_status()
    return response.json()


def get_series_metadata(per_page: int = 100) -> pd.DataFrame:
    """
    Get series metadata from the World Bank Indicator API.

    Returns
    -------
    pd.DataFrame
        Data with series metadata.
    """
    data = []
    with httpx.Client() as client:
        metadata, indicators = __get_series_metadata(client, 1, per_page)
        data.extend(indicators)
        for page in tqdm(range(2, metadata["pages"])):
            _, indicators = __get_series_metadata(client, page, per_page)
            data.extend(indicators)
    return pd.DataFrame(data)


def __get_series_data(
    client: httpx.Client, series_id: str, page: int, per_page: int, **kwargs
) -> pd.DataFrame | None:
    """
    Get a single series data page.
    """
    response = client.get(
        urljoin(BASE_URL, f"country/all/indicator/{series_id}"),
        params={
            "date": "2015:2025",
            "page": page,
            "per_page": per_page,
            "format": "json",
        },
    )
    response.raise_for_status()
    metadata, data = None, None
    if len(data := response.json()) == 1:
        metadata = data[0]
        if "message" in metadata:
            print(metadata)
        metadata = None
    elif len(data) == 2:
        metadata, data = data
    return metadata, data


def get_series_data(series_id: str, **kwargs) -> pd.DataFrame | None:
    """
    Get series data from the the World Bank Indicator API.

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
    data = []
    with httpx.Client(timeout=30) as client:
        metadata, records = __get_series_data(client, series_id, page=1, per_page=100)
        data.extend(records)
        if metadata is not None:
            for page in tqdm(range(2, metadata["pages"])):
                metadata, records = __get_series_data(
                    client, series_id, page=page, per_page=100
                )
                data.extend(records)
        else:
            data = None
    return pd.DataFrame(data)
