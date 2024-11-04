"""retrieve series and metadata via api"""

import asyncio
from collections import defaultdict
from typing import Any, DefaultDict
from urllib.parse import urljoin

import aiohttp
import requests

from dfpp.sources.un_org.utils import flatten_dict

__all__ = ["get_indicator_list", "get_series_data_and_dimensions"]

BASE_URL = "https://unstats.un.org/sdgapi/v1/sdg/"


def get_indicator_list() -> list[dict]:
    """Get list of the indicators

    Returns:
        list[dict]: A list of dictionaries containing the indicator data
    """
    url = urljoin(BASE_URL, "Series/List?allreleases=false")

    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    return data


async def get_data(
    session: aiohttp.ClientSession,
    series_id: str,
    page_number: int = 1,
) -> dict[str, Any]:
    """Send a GET request to the API
    to get one page of series data from a paginated series data endpoint.

    Args:
        session (aiohttp.ClientSession): The client session to use for the request.
        series_id (str): The series id.
        page_number (int, optional): The page number. Defaults to 1.

    Returns:
        dict[str, Any]: The response data.
    """
    url = urljoin(BASE_URL, "Series/Data")
    params = {"seriesCode": series_id, "pageSize": 1000, "page": page_number}
    headers = {"Accept": "application/json"}

    async with session.get(url, headers=headers, params=params) as response:
        response.raise_for_status()
        return await response.json()


async def get_series_data(
    series_id: str, session: aiohttp.ClientSession
) -> tuple[str, list[dict], list[str], int]:
    """
    Get series data from the api, return series id, data, dimensions and size.

    Args:
        series_id (str): The series id.
        session (aiohttp.ClientSession): The client session.

    Returns:
        tuple[str, list[dict], list[str], int]: A tuple containing the series id, data, dimensions and size.
    """
    all_pages: list[dict] = []

    data: dict = await get_data(session, series_id, page_number=1)
    all_pages.extend(data["data"])
    total_pages: int = data["totalPages"]
    if total_pages > 1:
        for page in range(2, total_pages + 1):
            data = await get_data(session, series_id, page)
            all_pages.extend(data["data"])

    dimensions: list[str] = flatten_dict(
        [{d["id"]: d["codes"]} for d in data["dimensions"]]
    )
    attributes: list[str] = flatten_dict(
        [{a["id"]: a["codes"]} for a in data["attributes"]]
    )
    size: int = data["totalElements"]

    return series_id, all_pages, dimensions, attributes, size


async def get_series_data_and_dimensions(
    series_codes: list[str],
) -> tuple[DefaultDict[str, list[dict]], DefaultDict[str, dict]]:
    """wrapper to get the series data and metadata"""
    series_data_map: DefaultDict[str, list[dict]] = defaultdict(list)
    series_map: DefaultDict[str, dict] = defaultdict(dict)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for row in series_codes:
            series_id = row
            tasks.append(get_series_data(series_id, session))

        results = await asyncio.gather(*tasks)

        for series_id, data, dimensions, attributes, size in results:
            series_data_map[series_id] = data
            series_map[series_id]["attributes"] = attributes
            series_map[series_id]["dimensions"] = dimensions
            series_map[series_id]["totalElements"] = size

    return series_data_map, series_map
