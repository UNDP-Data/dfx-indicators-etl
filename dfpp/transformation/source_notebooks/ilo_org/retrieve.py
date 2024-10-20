"""retrieve series and metadata via api frin ILO Rplumber API"""

from collections import defaultdict
from io import BytesIO
import aiohttp
import pandas as pd
import requests
import gzip
from urllib.parse import urljoin

__all__ = [
    "get_indicator",
    "list_indicators",
    "get_codebook",
    "download_indicator_file",
    "read_to_df_csv_indicator",
]

BASE_URL = "https://rplumber.ilo.org/"


def list_indicators():
    """get indicators and their metadata"""
    url = urljoin(BASE_URL, "metadata/toc/indicator/?lang=en&format=json")
    headers = {"accept": "application/octet-stream"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return {key.replace(".", "_"): value for key, value in data.items()}


def get_codebook():
    """Get the ILO codes and their descriptions as DataFrames with column names sanitized"""
    data_codes = defaultdict()
    codes = [
        "classif1",
        "classif2",
        "obs_status"
    ]

    for code in codes:
        url = urljoin(BASE_URL, f"metadata/dic/?var={code}&format=.json")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data)
        df.columns = df.columns.str.replace(".", "_")
        data_codes[code] = df

    return data_codes


async def get_indicator(indicator_id):
    """Fetch indicator data asynchronously for the given indicator ID"""
    url = urljoin(BASE_URL, f"data/indicator/?id={indicator_id}&format=json")
    headers = {"accept": "application/octet-stream"}

    async with aiohttp.ClientSession() as session:
        response = await session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=60)
        )
        async with response:
            response.raise_for_status()
            content = await response.read()
            try:
                data = await response.json(content_type=None)
                return data
            except Exception as e:
                print(f"Failed to decode JSON: {e}")
                return content


def get_bulk_download_indicator_list() -> pd.DataFrame:
    """Get list of available indicators via bulk download"""
    table_of_contents_url = "https://webapps.ilo.org/ilostat-files/WEB_bulk_download/indicator/table_of_contents_en.csv"
    df_bulk_indicators = pd.read_csv(table_of_contents_url)
    df_bulk_indicators.columns = df_bulk_indicators.columns.str.replace(".", "_")
    df_bulk_indicators = df_bulk_indicators[df_bulk_indicators.freq == "A"]
    return df_bulk_indicators


async def download_indicator_file(indicator_id: str) -> bytes:
    """alternative download method if RPLUMBER API fails

    :param indicator_id: the ILO indicator ID
    :return: the contents of the file as bytes
    """
    base_url = f"https://webapps.ilo.org/ilostat-files/WEB_bulk_download/indicator/{indicator_id}.csv.gz"
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url) as response:
            response.raise_for_status()
            return await response.read()


def read_to_df_csv_indicator(file: bytes) -> pd.DataFrame:
    """
    Reads a gzipped CSV file into a pandas DataFrame.
    """
    with gzip.GzipFile(fileobj=BytesIO(file)) as gz:
        df = pd.read_csv(gz, low_memory=False)
        return df
