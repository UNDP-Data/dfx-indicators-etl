from collections import defaultdict

import aiohttp
import pandas as pd
import requests

__all__ = ["get_indicator", "list_indicators", "get_codebook"]

def list_indicators():
    """get indicators and their metadata"""
    url = "https://rplumber.ilo.org/metadata/toc/indicator/?lang=en&format=json"

    headers = {"accept": "application/octet-stream"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return {key.replace(".", "_"): value for key, value in data.items()}


def get_codebook():
    """Get the ILO codes and their descriptions as DataFrames with column names sanitized"""
    data_codes = defaultdict()
    codes = [
        "ref_area",
        "indicator",
        "sex",
        "classif1",
        "classif2",
        "obs_status",
        "note_classif",
        "note_indicator",
        "note_source",
    ]

    for code in codes:
        url = f"https://rplumber.ilo.org/metadata/dic/?var={code}&format=.json"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data)
        df.columns = df.columns.str.replace(".", "_")
        data_codes[code] = df

    return (
        data_codes["ref_area"],
        data_codes["indicator"],
        data_codes["sex"],
        data_codes["classif1"],
        data_codes["classif2"],
        data_codes["obs_status"],
        data_codes["note_classif"],
        data_codes["note_indicator"],
        data_codes["note_source"],
    )


async def get_indicator(indicator_id):
    """Fetch indicator data asynchronously for the given indicator ID"""
    url = f"https://rplumber.ilo.org/data/indicator/?id={indicator_id}&format=json"

    headers = {"accept": "application/octet-stream"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            content = await response.read()

            try:
                data = await response.json(content_type=None)
                return data
            except Exception as e:
                print(f"Failed to decode JSON: {e}")
                return content
