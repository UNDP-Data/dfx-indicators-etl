"""
ETL components to process data from the ILOSTAT SDMX API.
See https://ilostat.ilo.org/resources/sdmx-tools/.
"""
import logging
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
from ..validation import PREFIX_DIMENSION
from ._base import BaseRetriever, BaseTransformer
logger = logging.getLogger(__name__)
__all__ = ["Retriever", "Transformer"]

# 🛠️ FIX 1: Updated to the new SDMX 2.1 base URL
BASE_URL = "https://sdmx.ilo.org/rest/"
DIMENSIONS = {"SEX", "AGE", "GEO", "EDU", "NOC"}


# 🛠️ FIX 4: Added lru_cache.
# This prevents identical HTTP requests for codelists during the Transform loop!
@lru_cache(maxsize=32)
def _get_codelist_mapping_original(name: str) -> dict:
    """
    Get codelist mapping from IDs to names from the ILO SDMX API codelist endpoint.
    """
    response = httpx.get(urljoin(BASE_URL, f"codelist/ILO/CL_{name}"), timeout=15)
    response.raise_for_status()

    xml = StringIO(response.text)
    namespaces = dict([node for _, node in ET.iterparse(xml, events=["start-ns"])])
    namespaces["xml"] = "http://www.w3.org/XML/1998/namespace"

    root = ET.fromstring(response.text)
    return {
        element.get("id"): element.find("common:Name[@xml:lang='en']", namespaces).text
        for element in root.findall(".//structure:Code", namespaces)
    }


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
    marked_del = []
    mapping = {}
    for element in root.findall(".//structure:Code", namespaces):
        name_node = element.find("common:Name[@xml:lang='en']", namespaces)
        if name_node is not None:
            name_text = name_node.text
            # 🛠️ FIX: Skip the deprecated codes marked as 'DEL'
            if name_text and name_text.strip().upper() != 'DEL':
                mapping[element.get("id")] = name_text
            else:
                marked_del.append(element.get("id"))

    return mapping, marked_del

class Retriever(BaseRetriever):
    """
    A class for retrieving data from the ILOSTAT SDMX API.
    """

    uri: HttpUrl = Field(default=BASE_URL, frozen=True, validate_default=True)

    def __call__(self, **kwargs) -> pd.DataFrame:
        # mapping= _get_codelist_mapping_original("INDICATOR")
        #
        # logger.info(f'Original codelist func: {len(mapping)} indicators')
        # mapping, deleted  = _get_codelist_mapping("INDICATOR")
        #
        # logger.info(f'New codelist func: {len(mapping)} indicators')
        df_metadata = self.get_metadata()


        # 🛠️ FIX 2: Strip 'DF_' prefix before applying the strict slice(2, -1) logic.
        # This ensures the dimension matching works whether ILO returns DF_ or not.
        mask = (
            df_metadata["code"]
            .str.replace("^DF_", "", regex=True)
            .str.split("_")
            .str.slice(2, -1)
            .apply(lambda x: not set(x) - DIMENSIONS)
        )
        df_metadata = df_metadata.loc[mask].reset_index(drop=True)
        data = []
        next_rotation = random.randint(5, 15)  # Start with a random window between 5 and 15
        requests_since_rotation = 0
        with self.client as client:

            # 🛠️ FIX 3: Inject the official SDMX-CSV Accept header
            client.headers.update({"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"})

            for i, (_, row) in enumerate(pbar:=tqdm(df_metadata.iterrows(), total=len(df_metadata), ncols=150)):
                # Check if we've hit our current random threshold
                if requests_since_rotation >= next_rotation:
                    ua = generate_user_agent(os='linux', device_type='desktop')
                    client.headers.update({"User-Agent": ua})

                    # Reset the counter and pick a NEW random window for the next batch
                    requests_since_rotation = 0
                    next_rotation = random.randint(8, 20)  # Pick a new random gap

                    pbar.set_description(f"UA Rotated! Next in {next_rotation}")


                df = self._get_data(row.code, client=client, **kwargs)
                if df is None or df.empty:
                    pbar.set_description(f'{row["code"]} returned no data ')
                    requests_since_rotation+=1
                    continue
                df["indicator_name"] = f"{row['name']} [{row['code']}]"
                data.append(df)
                pbar.set_description(f'{row["code"]} returned {len(df)} rows')
                requests_since_rotation+=1

        return pd.concat(data, axis=0, ignore_index=True) if data else pd.DataFrame()

    def _get_metadata_original(self) -> pd.DataFrame:
        """
        Get indicator metadata from the ILO SDMX API codelist endpoint.

        Returns
        -------
        pd.DataFrame
            Data frame with two columns `code` and `name`.
        """
        mapping = _get_codelist_mapping("INDICATOR")
        df = pd.DataFrame(mapping.items(), columns=["code", "name"])
        return df

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

        # 🛠️ FIX 2 (cont): Ensure the Dataflow ID explicitly has the DF_ prefix
        df_code = indicator_code if indicator_code.startswith("DF_") else f"DF_{indicator_code}"

        # Using the standard SDMX data endpoint structure
        return self.read_csv(f"data/ILO,{df_code}/", params, client)


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