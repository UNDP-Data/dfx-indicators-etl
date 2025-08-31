"""
Functions to retrieve data from UNICEF SDMX API.
See https://data.unicef.org/sdmx-api-documentation/ and
https://sdmx.data.unicef.org/overview.html.
"""

import httpx
import pandas as pd

__all__ = ["get_series_metadata", "get_query_options", "get_series_data"]

BASE_URL = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest"


def _get_dataflow(name: str) -> str:
    params = {
        "format": "fusion-json",
        "dimensionAtObservation": "AllDimensions",
        "detail": "structureOnly",
        "includeMetrics": True,
        "includeMetadata": True,
        "match": "all",
        "includeAllAnnotations": True,
    }
    response = httpx.get(f"{BASE_URL}/data/{name}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def get_query_options(dataflow: str) -> list[str]:
    data = _get_dataflow(dataflow)
    observation = data["structure"]["dimensions"]["observation"]
    return [x["id"].lower() for x in observation]


def get_series_metadata(dataflow: str) -> pd.DataFrame:
    """
    Get series metadata from UNICEF Indicator Data Warehouse.

    Returns
    -------
    pd.DataFrame
        Data frame with metadata columns.
    """
    to_rename = {
        "id": "series_id",
        "name": "series_name",
        "description": "series_description",
    }
    data = _get_dataflow(dataflow)
    observation = data["structure"]["dimensions"]["observation"]
    indicators = [x for x in observation if x["id"] == "INDICATOR"][0]["values"]
    indicators = [indicator for indicator in indicators if indicator["inDataset"]]
    return pd.DataFrame(indicators).reindex(columns=to_rename).rename(columns=to_rename)


def get_series_data(dataflow: str, **kwargs):
    """
    Get series data from UNICEF Indicator Data Warehouse.

    Parameters
    ----------
    dataflow : str
        Dataflow to retrieve data from.
    **kwargs
        Dataflow-specific keyword arguments that typically include
        indicator IDs, geographic area etc. Check `get_query_options`
        for valid values.

    Returns
    -------
    pd.DataFrame or None
        Data frame with country data in the wide format.

    Examples
    --------
    >>> get_series_data(
    ...    "UNICEF,GLOBAL_DATAFLOW,1.0",
    ...    indicator="DM_POP_TOT",
    ...    time_period=["2020", "2021"],
    ... )
    #       REF_AREA Geographic area INDICATOR  ... Current age
    # 0     AFG      Afghanistan	 DM_POP_TOT ... Total
    # ...
    # 69041 ZWE      Zimbabwe        DM_POP_TOT ... Total
    """
    options = get_query_options(dataflow)
    if set(options) & set(kwargs):
        values = []
        for option in options:
            value = kwargs.get(option, "")
            if isinstance(value, str):
                values.append(value)
            elif isinstance(value, list):
                values.append("+".join(value))
            else:
                raise ValueError(
                    f"{option} must be either a string or list of strings, got {type(value)}."
                )
        options = ".".join(values)
    else:
        options = "all"
    return pd.read_csv(f"{BASE_URL}/data/{dataflow}/{options}?format=csv&labels=both")
