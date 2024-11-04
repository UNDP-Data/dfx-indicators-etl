import asyncio
from urllib.parse import urljoin

import aiohttp
import pandas as pd
import requests

BASE_URL = "https://ghoapi.azureedge.net/api/"

__all__ = [
    "list_indicators",
    "get_dimension_value_codes",
    "get_indicator_data",
    "get_dimension_values",
]


def list_indicators():
    """Retrieve a list of indicators from the GHO API."""
    response = requests.get(urljoin(BASE_URL, "Indicator"))
    response.raise_for_status()
    return response.json()["value"]


def list_dimensions():
    """Retrieve a list of dimensions from the GHO API."""
    response = requests.get(urljoin(BASE_URL, "DIMENSION"))
    response.raise_for_status()
    return response.json()["value"]


async def get_all_dimension_values(df_dimensions: pd.DataFrame) -> pd.DataFrame:
    """Get values associated with each dimension available in WHO API.
    This version retrieves the dimension values and processes them separately."""
    raw_dimension_values = await get_dimension_values(df_dimensions)
    concatenated_data = pd.concat(raw_dimension_values, ignore_index=True)
    return concatenated_data


async def get_dimension_values(
    df_dimensions: pd.DataFrame, max_concurrency: int = 1
) -> list:
    """Asynchronously retrieve raw dimension values for each dimension with a semaphore to limit concurrent requests."""
    raw_dimension_data = []
    semaphore = asyncio.Semaphore(max_concurrency)

    async with aiohttp.ClientSession() as session:
        tasks = [
            get_dimension_value_codes(code, session, semaphore)
            for code in df_dimensions.Code.tolist()
        ]
        results = await asyncio.gather(*tasks)

        for code, dimension_values in results:
            if dimension_values:
                df_dimension_values = pd.DataFrame(dimension_values)
                raw_dimension_data.append(df_dimension_values)

    return raw_dimension_data


async def get_dimension_value_codes(dimension, session, semaphore):
    """Retrieve dimension values for a specific dimension code with a semaphore."""
    async with semaphore:
        async with session.get(
            urljoin(BASE_URL, f"DIMENSION/{dimension}/DimensionValues")
        ) as response:
            response.raise_for_status()
            data = await response.json()
            return dimension, data.get("value", [])


async def get_indicator_data(indicator_code):
    """Retrieve data for a specific indicator using its code."""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            urljoin(BASE_URL, indicator_code),
            timeout=aiohttp.ClientTimeout(total=60),
        ) as response:
            response.raise_for_status()
            return await response.json()
