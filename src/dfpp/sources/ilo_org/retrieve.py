"""
Functions to retrieve data from ILOSTAT SDMX tools.
See https://ilostat.ilo.org/resources/sdmx-tools/.
"""

import xml.etree.ElementTree as ET
from io import StringIO
from urllib.parse import urlencode, urljoin
from urllib.request import HTTPError

import httpx
import pandas as pd

__all__ = [
    "DISAGGREGATIONS",
    "get_codelist_mapping",
    "get_series_metadata",
    "get_series_data",
]

BASE_URL = "https://sdmx.ilo.org/rest/"
HEADERS = {"User-Agent": "pandas"}
DISAGGREGATIONS = {"SEX", "AGE", "GEO", "EDU", "NOC"}


def get_codelist_mapping(name: str) -> dict:
    """
    Get codelist mapping from IDs to names from the ILO SDMX API codelist endpoint.

    Parameters
    ----------
    name : str
        Name of the codelist, such as "AGE", "SEX", "GEO".

    Returns
    -------
    dict
        Mapping from IDs to names.
    """
    response = httpx.get(urljoin(BASE_URL, f"codelist/ILO/CL_{name.upper()}"))
    response.raise_for_status()
    # Create a file-like object to extract namespace
    xml = StringIO(response.text)
    namespaces = dict([node for _, node in ET.iterparse(xml, events=["start-ns"])])
    # Add the XML namespace for xml:lang
    namespaces["xml"] = "http://www.w3.org/XML/1998/namespace"

    # Parse the XML
    root = ET.fromstring(response.text)
    return {
        element.get("id"): element.find("common:Name[@xml:lang='en']", namespaces).text
        for element in root.findall(".//structure:Code", namespaces)
    }


def get_series_metadata() -> pd.DataFrame:
    """
    Get series metadata from the ILO SDMX API codelist endpoint.

    Returns
    -------
    pd.DataFrame
        Data frame with two columns `series_code` and `series_name`.
    """
    mapping = get_codelist_mapping("INDICATOR")
    return pd.DataFrame(mapping.items(), columns=["series_code", "series_name"])


def get_series_data(
    series_code: str, period_from: str = "2015-01-01", period_to: str = "2025-12-31"
) -> pd.DataFrame | None:
    """
    Get series data from the ILO SDMX API data endpoint.

    Parameters
    ----------
    series_code : str
        Series code. See `get_series_metadata`.
    period_from : str, default="2015-01-01"
        Get data from this date.
    period_to : str, default="2025-12-31"
        Get data until this date.

    Returns
    -------
    pd.DataFrame or None
        Data frame with data as returned by the API or None of no data is present.
    """
    params = {"format": "csvfile", "startPeriod": period_from, "endPeriod": period_to}
    try:
        return pd.read_csv(
            urljoin(BASE_URL, f"data/ILO,{series_code}/?{urlencode(params)}"),
            storage_options=HEADERS,
            low_memory=False,
        )
    except pd.errors.EmptyDataError:
        return None
    except HTTPError as error:
        print(series_code, error)
        return None
