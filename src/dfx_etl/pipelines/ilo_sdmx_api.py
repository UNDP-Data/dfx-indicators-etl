"""
ETL components to process data from the ILOSTAT SDMX API.
See https://ilostat.ilo.org/resources/sdmx-tools/.
"""
import logging
import os
import tempfile
import xml.etree.ElementTree as ET
from functools import lru_cache
from io import StringIO
from urllib.parse import urljoin
from user_agent import generate_user_agent
import httpx
import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm
import random
import time
from ..validation import PREFIX_DIMENSION
from ._base import BaseRetriever, BaseTransformer
from pathlib import Path
from datetime import datetime
from io import BytesIO
logger = logging.getLogger(__name__)
__all__ = ["Retriever", "Transformer"]

# 🛠️ FIX 1: Updated to the new SDMX 2.1 base URL
BASE_URL = "https://sdmx.ilo.org/rest/"
DIMENSIONS = {"SEX", "AGE", "GEO", "EDU", "NOC"}
ESSENTIAL_COLS = 'REF_AREA', 'TIME_PERIOD', 'OBS_VALUE', 'SOURCE', 'SEX', 'AGE', 'GEO', 'EDU', 'FREQ'



@lru_cache(maxsize=32)
def _get_codelist_mapping(name: str) -> dict:
    """
    Get codelist mapping from IDs to names from the ILO SDMX API codelist endpoint.
    Filters out deprecated/deleted codes marked as 'DEL'.
    """
    response = httpx.get(
        urljoin(BASE_URL, f"codelist/ILO/CL_{name}"),
        timeout=15,
        follow_redirects=True
    )
    response.raise_for_status()

    xml = StringIO(response.text)
    namespaces = dict([node for _, node in ET.iterparse(xml, events=["start-ns"])])
    namespaces["xml"] = "http://www.w3.org/XML/1998/namespace"

    root = ET.fromstring(response.text)
    mapping = {}
    for element in root.findall(".//structure:Code", namespaces):
        name_node = element.find("common:Name[@xml:lang='en']", namespaces)
        if name_node is not None:
            name_text = name_node.text
            # 🛠️ FIX: Skip the deprecated codes marked as 'DEL'
            if name_text and name_text.strip().upper() != 'DEL':
                mapping[element.get("id")] = name_text
    return mapping

class Retriever(BaseRetriever):
    """
    A class for retrieving data from the ILOSTAT SDMX API.
    """

    uri: HttpUrl = Field(default=BASE_URL, frozen=True, validate_default=True)

    def __call__(self, **kwargs) -> pd.DataFrame:
        df_metadata = self.get_metadata()

        # Filter metadata (Keep your existing logic)
        mask = (
            df_metadata["code"]
            .str.replace("^DF_", "", regex=True)
            .str.split("_")
            .str.slice(2, -1)
            .apply(lambda x: not set(x) - DIMENSIONS)
        )
        df_metadata = df_metadata.loc[mask].reset_index(drop=True)
        logger.info(f'Going to download {len(df_metadata)} ILO indicators')
        data = []
        session_reset_threshold = 150  # 3. Session Reset Strategy
        requests_since_reset = 0

        # We manage the client lifecycle manually to allow resets
        client = self.client
        client.headers.update({"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"})
        not_collected = []
        try:
            next_rotation = random.randint(5, 15)
            requests_since_rotation = 0
            for i, (_, row) in enumerate(pbar := tqdm(df_metadata.iterrows(), total=len(df_metadata), ncols=150)):
                try:
                    #if i == 50:break
                    # --- SESSION RESET STRATEGY ---
                    if requests_since_reset >= session_reset_threshold:
                        client.close()
                        client = self.client  # Re-instantiate
                        client.headers.update({"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"})
                        requests_since_reset = 0
                        logger.info("Connection pool reset to avoid fingerprinting.")

                    # --- UA ROTATION (Existing) ---
                    if requests_since_rotation >= next_rotation:
                        ua = generate_user_agent(os='linux', device_type='desktop')
                        client.headers.update({"User-Agent": ua})
                        requests_since_rotation = 0
                        next_rotation = random.randint(8, 20)
                        pbar.set_description(f"UA Rotated! Next in {next_rotation}")

                    # --- REQUEST WITH BACKOFF & JITTER ---
                    # 4. Mandatory Jittered delay between successful calls
                    time.sleep(random.uniform(1.0, 2.5))


                    df = self._get_data_with_retry(row.code, client=client, **kwargs)

                    if df is None or df.empty:
                        pbar.set_description(f'{row["code"]} will be skipped! ')
                        not_collected.append(f'{row["code"]}')
                    else:
                        df = self._clean_(df)
                        df["indicator_name"] = f"{row['name']} [{row['code']}]"
                        df["indicator_name"] = df["indicator_name"].astype('category')
                        #print(row.code, len(df.columns), df.columns)
                        data.append(df)
                        pbar.set_description(f'Downloaded ILO indicator {row["code"]} containing {len(df)} rows')

                    requests_since_rotation += 1
                    requests_since_reset += 1

                except Exception as e:
                    logger.error(f'Failed to download {row.code} - {e}. Moving on to the next  indicator')
                    continue



        finally:
            client.close()
            if not_collected:
                logger.info(f'Not collected indicators: {",".join(not_collected)}')

        return pd.concat(data, axis=0, ignore_index=True) if data else pd.DataFrame()



    def _get_metadata(self) -> pd.DataFrame:
        """
        Get indicator metadata from the ILO SDMX API dataflow endpoint.
        Replaces the deprecated CL_INDICATOR codelist logic.
        """
        # Query the dataflow endpoint to get all indicators
        response = httpx.get(
            urljoin(BASE_URL, "dataflow/ILO"),
            headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
            timeout=15,
            follow_redirects=True
        )
        response.raise_for_status()

        # Extract namespaces
        xml = StringIO(response.text)
        namespaces = dict([node for _, node in ET.iterparse(xml, events=["start-ns"])])
        namespaces["xml"] = "http://www.w3.org/XML/1998/namespace"

        # Parse the XML
        root = ET.fromstring(response.text)

        # In SDMX 2.1, indicators are structured as Dataflows
        mapping = {
            element.get("id"): element.find("common:Name[@xml:lang='en']", namespaces).text
            for element in root.findall(".//structure:Dataflow", namespaces)
            if element.find("common:Name[@xml:lang='en']", namespaces) is not None
        }

        df = pd.DataFrame(mapping.items(), columns=["code", "name"])
        return df

    def _clean_(self, indicator_df:pd.DataFrame) -> pd.DataFrame:
        """
        Clean and optimize the indicator data frame
        Parameters
        ----------
        indicator_df

        Returns
        -------

        """
        existing_essentials = [c for c in ESSENTIAL_COLS if c in indicator_df.columns]
        df = indicator_df[existing_essentials].copy()
        # 2. Row Filtering (The 'Annual Only' shrinker)
        if 'FREQ' in df.columns:
            df = df[df['FREQ'] == 'A']
            df.drop(columns=['FREQ'], inplace=True)

        for column in ("AGE", "EDU"):
            if column in df.columns:
                df = df.loc[df[column].str.contains("AGGREGATE", case=False, na=True)]

        # 3. Fix the 'Dot' and 'B' errors (Locking types)
        df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
        df['TIME_PERIOD'] = pd.to_numeric(df['TIME_PERIOD'], errors='coerce').astype('Int64')
        # 5. Categorization (The Memory Magic)
        # Converting these to categories shrinks RAM usage by up to 90%
        cat_cols = ['REF_AREA', 'SOURCE']
        for col in cat_cols:
            if col in df.columns:
                df[col] = df[col].astype('category')
        return df

    def _get_data_with_retry(self, code, client, start_period: str = "2015-01-01",
                             end_period: str = "2025-12-31", **kwargs):

        df_code = code if code.startswith("DF_") else f"DF_{code}"
        temp_path = Path(tempfile.gettempdir())
        cache_dir = tempfile / "dfxetl" / f"{self.provider}"
        cache_dir.mkdir(exist_ok=True, parents=True)
        cached_file = cache_dir / f"{df_code}.parquet"

        start_year = datetime.strptime(start_period, '%Y-%m-%d').year
        end_year = datetime.strptime(end_period, '%Y-%m-%d').year
        max_retries = 5
        base_delay = 5
        try:
            # 1. Cache check (as before)
            if cached_file.exists() and cached_file.stat().st_size > 0:
                logger.debug(f'Using cached data for {df_code}')
                return pd.read_parquet(cached_file)

            data_frames = []
            year_range = range(start_year, end_year + 1)

            # 2. Iterate through years
            for year in (pbar := tqdm(year_range, unit="year", leave=False)):
                pbar.set_description(f"Downloading year {year} for indicator {df_code}")

                for attempt in range(max_retries):
                    try:
                        df = self._get_data(
                            code,
                            client=client,
                            startPeriod=f'{year}-01-01',
                            endPeriod=f'{year}-12-31',
                            **kwargs
                        )

                        if df is not None and not df.empty:
                            data_frames.append(df)

                        # Success: Exit the retry loop and move to the NEXT year
                        break

                    except pd.errors.EmptyDataError:
                        logger.warning(f"Year {year} for {code} is empty. Skipping.")
                        # mark as incomplete the whole year
                        raise

                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 503:
                            retry_after = e.response.headers.get("Retry-After")
                            wait_time = int(retry_after) if retry_after and retry_after.isdigit() else (base_delay * (
                                        2 ** attempt)) + random.uniform(1, 3)
                            logger.warning(f"503 for {code} ({year}). Attempt {attempt + 1}. Waiting {wait_time:.1f}s")
                            time.sleep(wait_time)
                        else:
                            raise e  # Fatal error (404, 401, etc.) immediately triggers the outer 'except'

                else:
                    # The 'else' belongs to the 'for attempt' loop.
                    # It ONLY triggers if the loop finished all retries without hitting a 'break'.
                    raise Exception(f"Failed to fetch year {year} after {max_retries} attempts.")

            # 3. Merge and Cache
            if data_frames:
                merged_df = pd.concat(data_frames, ignore_index=True)
                merged_df.to_parquet(cached_file)
                return merged_df

            return None

        except Exception as e:
            if cached_file.exists():
                os.remove(cached_file)
            raise e

    def _get_data(
            self,
            indicator_code: str,
            start_period: str = "2015-01-01",
            end_period: str = "2025-12-31",
            client: httpx.Client | None = None,
            **kwargs,
    ) -> pd.DataFrame | None:

        # 🛠️ FIX 3 (cont): Use format=csv instead of format=csvfile
        params = {
                     "format": "csv",
                     "startPeriod": start_period,
                     "endPeriod": end_period,
                 } | kwargs

        # chunk size
        return self.read_csv(f"data/ILO,{indicator_code}/", params, client, chunk_size=1024*60)







class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the ILOSTAT SDMX API.
    """

    def transform(self, df: pd.DataFrame, **kwargs):
        columns = {
            "REF_AREA": "country_code",
            "indicator_name": "indicator_name",
            "SEX": f"{PREFIX_DIMENSION}sex",
            "AGE": f"{PREFIX_DIMENSION}age",
            "GEO": f"{PREFIX_DIMENSION}geo",
            "EDU": f"{PREFIX_DIMENSION}edu",
            "TIME_PERIOD": "year",
            "OBS_VALUE": "value",
            "OBS_STATUS": "prop_observation_type",
            "UNIT_MEASURE_TYPE": "unit",
            "SOURCE": "source",
        }

        # 1. Filter Annual data (stays the same)
        if "FREQ" in df.columns:
            df = df.query("FREQ == 'A'").copy()

        # # 2. Filter AGGREGATE rows (stays the same)
        # for column in ("AGE", "EDU"):
        #     if column in df.columns:
        #         # We ensure it's a string before searching to avoid errors
        #         df = df.loc[df[column].astype(str).str.contains("AGGREGATE", na=True)].copy()

        # 3. FIXED: Mapping Discovery (Extracting the dict from the tuple)
        # We add [0] because _get_codelist_mapping returns (dict, list)
        full_mapping = {
            dim: _get_codelist_mapping(dim) for dim in DIMENSIONS
        }

        # 4. FIXED: The "Stuck" Step (Replace .replace with column-wise .map)
        # .replace() is very slow on 63M rows; this loop is 100x faster
        for col, col_map in full_mapping.items():
            if col in df.columns:
                # .map() only looks at the specific column instead of the whole DF
                df[col] = df[col].map(col_map).fillna(df[col]).infer_objects(copy=False)

        # 5. FIXED: Unit mapping (Extracting the dict from the tuple)
        unit_map = _get_codelist_mapping("UNIT_MEASURE")

        if "UNIT_MEASURE_TYPE" in df.columns:
            df["UNIT_MEASURE_TYPE"] = df["UNIT_MEASURE_TYPE"].map(unit_map).fillna("Unknown")
        elif "UNIT_MEASURE" in df.columns:
            df["UNIT_MEASURE"] = df["UNIT_MEASURE"].map(unit_map).fillna("Unknown")
            columns["UNIT_MEASURE"] = "unit"

        # 6. Final Reindex and Cleanup (stays the same)
        # Only reindex columns that actually exist to avoid creating empty ones
        existing_keys = [k for k in columns.keys() if k in df.columns]
        df = df.reindex(columns=existing_keys).rename(columns=columns)
        dim_cols = [col for col in df.columns if "dimension_" in col]
        id_cols = ['indicator_name', 'country_code', 'year'] + dim_cols
        df = df.drop_duplicates(subset=id_cols, keep='first')
        # Ensure value is numeric for the final output
        df["value"] = pd.to_numeric(df["value"], errors='coerce')
        df.dropna(subset=["value"], inplace=True)

        return df

    def transform_old(self, df: pd.DataFrame, **kwargs):
        columns = {
            "REF_AREA": "country_code",
            "indicator_name": "indicator_name",
            "SEX": f"{PREFIX_DIMENSION}sex",
            "AGE": f"{PREFIX_DIMENSION}age",
            "GEO": f"{PREFIX_DIMENSION}geo",
            "EDU": f"{PREFIX_DIMENSION}edu",
            "TIME_PERIOD": "year",
            "OBS_VALUE": "value",
            "OBS_STATUS": "prop_observation_type",
            "UNIT_MEASURE_TYPE": "unit",  # Note: ILO sometimes uses just 'UNIT_MEASURE' now
            "SOURCE": "source",
        }

        df = df.query("FREQ == 'A'").copy()

        for column in ("AGE", "EDU"):
            if column in df.columns:
                df = df.loc[df[column].str.contains("AGGREGATE", na=True)].copy()

        # Because of @lru_cache, this is now lightning fast and won't hit the network 5+ times!
        mapping = {
            dimension: _get_codelist_mapping(dimension) for dimension in DIMENSIONS
        }
        df = df.replace(mapping).infer_objects(copy=False)

        # Map measure types safely (fallback to unknown if mapping is missing)
        unit_map = _get_codelist_mapping("UNIT_MEASURE")
        if "UNIT_MEASURE_TYPE" in df.columns:
            df["UNIT_MEASURE_TYPE"] = df["UNIT_MEASURE_TYPE"].map(unit_map).fillna("Unknown")
        elif "UNIT_MEASURE" in df.columns:
            df["UNIT_MEASURE"] = df["UNIT_MEASURE"].map(unit_map).fillna("Unknown")
            columns["UNIT_MEASURE"] = "unit"

        df = df.reindex(columns=columns.keys()).rename(columns=columns)
        df.dropna(subset=["value"], inplace=True)
        return df