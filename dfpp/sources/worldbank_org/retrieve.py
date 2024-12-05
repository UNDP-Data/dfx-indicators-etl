"""retrieve series via api from world bank"""

from urllib.parse import urljoin
import logging

import httpx
import pandas as pd
from tqdm.asyncio import tqdm

__all__ = ["Connector"]

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

NOT_FOUND_MESSAGE = "The indicator was not found. It may have been deleted or archived."

BASE_URL = "https://api.worldbank.org/v2/"


class Connector:
    def __init__(self, connections: int = 10, timeout: int = 60) -> None:
        """
        Initialize the Connector with specified parameters.

        Args:
            connections (int): The maximum number of concurrent connections. Default is 10.
            timeout (int): The timeout for HTTP requests in seconds. Default is 60.

        Returns:
            None
        """
        self.base_url = BASE_URL
        self.connections = connections
        self.timeout = timeout
        self.limits = httpx.Limits(max_connections=self.connections)
        self.params = {
            "page": 1,
            "per_page": 100,
            "format": "json",
        }

    async def get_results(self, url: str) -> pd.DataFrame:
        """
        Retrieve data from World Bank API.

        Args:
            url (str): The URL of the API endpoint.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the retrieved data.
        """
        async with httpx.AsyncClient(
            timeout=self.timeout, limits=self.limits
        ) as client:
            response = await client.get(url, params=self.params)
            response_content = response.json()
            if (
                len(response_content) == 1
                and response_content[0]["message"][0]["value"] == NOT_FOUND_MESSAGE
            ):
                logging.warning(NOT_FOUND_MESSAGE)
                return pd.DataFrame()
            metadata, data = response_content
            pages = range(2, metadata["pages"] + 1)
            tasks = [
                client.get(url, params=self.params | {"page": page}) for page in pages
            ]
            for response in await tqdm.gather(*tasks):
                _, records = response.json()
                data.extend(records)
        df = pd.DataFrame(data)
        return df

    async def get_indicators(self) -> pd.DataFrame:
        """
        Retrieve a DataFrame containing indicators.

        Returns:
            pd.DataFrame: A DataFrame with columns 'id' and 'name'.
        """
        url: str = urljoin(self.base_url, "indicator")
        df: pd.DataFrame = await self.get_results(url)
        df = df.reindex(columns=["id", "name"])
        return df

    async def get_series(self, indicator_id: str) -> pd.DataFrame:
        """
        Retrieve a DataFrame containing time series for an indicator.

        Args:
            indicator_id (str): The World Bank indicator ID.

        Returns:
            pd.DataFrame: A DataFrame with the time series data.
        """
        url = urljoin(self.base_url, f"country/all/indicator/{indicator_id}")
        df = await self.get_results(url)
        return df
