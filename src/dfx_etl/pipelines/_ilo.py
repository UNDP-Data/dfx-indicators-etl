import pandas as pd
import httpx
import logging
import io
from typing import List
from pydantic import Field, HttpUrl
from ..validation import PREFIX_DIMENSION
from ._base import BaseRetriever, BaseTransformer
logger = logging.getLogger(__name__)

# Keep your original dimension whitelist
DIMENSIONS = {"SEX", "AGE", "GEO", "EDU", "NOC"}
BASE_URL = "https://rplumber.ilo.org"


class Retriever(BaseRetriever):

    uri: HttpUrl = Field(default=BASE_URL, frozen=True, validate_default=True)


    def __call__(self, *args, **kwargs)->pd.DataFrame:

        pass

    def _get_metadata(self) -> pd.DataFrame:
        """Ported logic: Fetch ToC and apply the dimension mask."""
        url = f"{self.uri}/metadata/toc/indicator/"
        response = httpx.get(url, timeout=30)
        response.raise_for_status()

        # Plumber ToC returns 'id' and 'indicator.label'
        df = pd.DataFrame(response.json())

        # Apply your colleague's specific logic:
        # Split ID and check if dimensions are within the allowed set
        def is_valid_dimension(indicator_id):
            # Example: UNE_2EAP_SEX_AGE_RT_A -> ['SEX', 'AGE']
            parts = indicator_id.split("_")[2:-1]
            return not set(parts) - DIMENSIONS

        mask = df["id"].apply(is_valid_dimension)
        return df.loc[mask].reset_index(drop=True)

    async def get_data(self, indicator_id: str) -> pd.DataFrame:
        """Bulk download replaces the year-by-year loop."""
        url = f"{self.uri}/data/indicator/"
        params = {
            "id": indicator_id,
            "format": ".csv",
            "type": "both"  # Returns 'sex' (code) AND 'sex.label' (name)
        }

        async with httpx.AsyncClient() as client:
            # High timeout because Plumber files are large
            response = await client.get(url, params=params, timeout=120.0)
            if response.status_code == 200:
                return pd.read_csv(io.StringIO(response.text))
            return pd.DataFrame()


class Transformer(BaseTransformer):

    def transform(self, df: pd.DataFrame, **kwargs):
        # Your colleague's column mapping
        # Note: Plumber uses lowercase for codes and .label for names
        columns = {
            "ref_area": "country_code",
            "sex.label": "prop_sex",  # mapped from labels now
            "age.label": "prop_age",
            "geo.label": "prop_geo",
            "edu.label": "prop_edu",
            "time": "year",
            "obs_value": "value",
            "source.label": "source",
        }

        if df.empty:
            return df

        # 1. Frequency filter (Annual)
        if "freq" in df.columns:
            df = df[df["freq"] == "A"].copy()

        # 2. Ported Aggregate Logic: Only keep AGGREGATE rows for AGE and EDU
        # In Plumber, we check the 'code' column for the string 'AGGREGATE'
        for col in ("age", "edu"):
            if col in df.columns:
                # Keep if contains AGGREGATE or if it's the only data available
                df = df[df[col].str.contains("AGGREGATE", na=True)]

        # 3. Rename and Reindex
        # We find which columns from our map actually exist in the bulk file
        existing_cols = {k: v for k, v in columns.items() if k in df.columns}
        df = df.rename(columns=existing_cols)

        # Cleanup
        df.dropna(subset=["value"], inplace=True)
        return df