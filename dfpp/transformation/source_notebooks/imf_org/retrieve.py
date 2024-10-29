"""retrive ILO datamapper API data"""

import httpx
import pandas as pd
import json
from urllib.parse import urljoin


BASE_URL = "https://www.imf.org/external/datamapper/api/v1/"

__all__ = ["list_indicators", "get_indicator_data"]


def list_indicators() -> pd.DataFrame:
    """Retrieve a DataFrame containing a list of indicators and their metadata.

    Returns:
        pd.DataFrame: A DataFrame with the indicators' metadata, including their IDs.
    """
    url: str = urljoin(BASE_URL, "indicators")
    response: httpx.Response = httpx.get(url)
    response.raise_for_status()
    data: dict = response.json()
    indicators: dict = data.get("indicators", {})

    indicator_list: list[dict] = []
    for key, value in indicators.items():
        value["id"] = key
        indicator_list.append(value)

    df: pd.DataFrame = pd.DataFrame(indicator_list)
    return df


async def get_indicator_data(client: httpx.AsyncClient,
    indicator_id: str,
) -> pd.DataFrame:
    response = await client.get(urljoin(BASE_URL, indicator_id))

    response.raise_for_status()
    response_body = json.loads(response.text)
    response_values = response_body.get("values")

    if not response_values:
        return pd.DataFrame()

    df_indicator = pd.DataFrame.from_records(response_values[indicator_id]).sort_index()

    return df_indicator
