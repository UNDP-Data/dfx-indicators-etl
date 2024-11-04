from urllib.parse import urljoin

import httpx
import pandas as pd
from tqdm.asyncio import tqdm

__all__ = ["Connector"]


class Connector:
    def __init__(self, connections: int = 10, timeout: int = 60):
        self.base_url = "https://api.worldbank.org/v2/"
        self.connections = connections
        self.timeout = timeout
        self.limits = httpx.Limits(max_connections=self.connections)
        self.params = {
            "page": 1,
            "per_page": 100,
            "format": "json",
        }

    async def get_results(self, url: str) -> pd.DataFrame:
        async with httpx.AsyncClient(
            timeout=self.timeout, limits=self.limits
        ) as client:
            response = await client.get(url, params=self.params)
            metadata, data = response.json()
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
        url = urljoin(self.base_url, "indicator")
        df = await self.get_results(url)
        df = df.reindex(columns=["id", "name"])
        return df

    async def get_series(self, indicator_id: str) -> pd.DataFrame:
        url = urljoin(self.base_url, f"country/all/indicator/{indicator_id}")
        df = await self.get_results(url)
        return df
