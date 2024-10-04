"""functions to retrieve the data from GHO WHO API"""

import aiohttp
import asyncio
import re
import requests
import pandas as pd


BASE_URL = "https://ghoapi.azureedge.net/api"

__all__ = [
    "list_indicators",
    "get_dimensions",
    "get_dimension_codes",
    "get_indicator_data",
    "get_dimension_map",
]


def sanitize_category(s):
    """sanitize column names"""
    s = s.split(":")[0]
    s = s.lower()
    s = re.sub(r"[()\[\]]", "", s)
    s = re.sub(r"[\s\W]+", "_", s)
    s = s.strip("_")
    return s


def list_indicators():
    """Retrieve a list of indicators from the GHO API."""
    response = requests.get(f"{BASE_URL}/Indicator")
    response.raise_for_status()
    return pd.DataFrame(response.json()["value"])


def get_dimensions():
    """Retrieve a list of dimensions from the GHO API."""
    response = requests.get(f"{BASE_URL}/DIMENSION")
    response.raise_for_status()

    df_dimensions = pd.DataFrame(response.json()["value"])
    df_dimensions.columns = [sanitize_category(s) for s in df_dimensions.columns]
    df_dimensions["dimension_to_display"] = df_dimensions["title"].apply(sanitize_category)
    return df_dimensions


async def get_dimension_map(df_dimensions: pd.DataFrame) -> pd.DataFrame:
    """get values associated with each dimension available in WHO API
    includes value codes, value titles (human readable) and dimension name"""
    to_concat_dimensions = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for code in df_dimensions.code.tolist():
            tasks.append(get_dimension_codes_async(code, session))
        results = await asyncio.gather(*tasks)

        for code, dimension_values in results:
            if not dimension_values["value"]:
                continue

            df_dimension_values = pd.DataFrame(dimension_values["value"])

            to_concat_dimensions.append(df_dimension_values)

    df_dimension_values = pd.concat(to_concat_dimensions)
    
    df_dimension_values = df_dimension_values[["Code", "Title", "Dimension"]]
    df_dimension_values.columns = [
        sanitize_category(s) for s in df_dimension_values.columns
    ]
    return df_dimension_values


async def get_dimension_codes_async(dimension, session):
    async with session.get(
        f"{BASE_URL}/DIMENSION/{dimension}/DimensionValues"
    ) as response:
        response.raise_for_status()
        return dimension, await response.json()


def get_dimension_codes(dimension):
    """Retrieve dimension values for a specific dimension code."""
    response = requests.get(f"{BASE_URL}/DIMENSION/{dimension}/DimensionValues")
    response.raise_for_status()
    return response.json()


async def get_indicator_data(indicator_code):
    """Retrieve data for a specific indicator using its code."""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{BASE_URL}/{indicator_code}", timeout=aiohttp.ClientTimeout(total=60)
        ) as response:
            response.raise_for_status()
            return await response.json()
