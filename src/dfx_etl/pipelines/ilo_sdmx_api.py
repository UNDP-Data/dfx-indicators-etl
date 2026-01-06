"""
ETL components to process data from the ILOSTAT SDMX API.
See https://ilostat.ilo.org/resources/sdmx-tools/.
"""
import logging
import xml.etree.ElementTree as ET
from io import StringIO
from urllib.parse import urljoin

import httpx
import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm

from ._base import BaseRetriever, BaseTransformer



__all__ = ["Retriever", "Transformer"]

BASE_URL = "https://sdmx.ilo.org/rest/"
DISAGGREGATIONS = {"SEX", "AGE", "GEO", "EDU", "NOC"}
logger = logging.getLogger(__name__)

def _get_codelist_mapping(name: str) -> dict:
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
    response = httpx.get(urljoin(BASE_URL, f"codelist/ILO/CL_{name}"), timeout=10)
    response.raise_for_status()
    # create a file-like object to extract namespace
    xml = StringIO(response.text)
    namespaces = dict([node for _, node in ET.iterparse(xml, events=["start-ns"])])
    # add the XML namespace for xml:lang
    namespaces["xml"] = "http://www.w3.org/XML/1998/namespace"
    # parse the XML
    root = ET.fromstring(response.text)
    return {
        element.get("id"): element.find("common:Name[@xml:lang='en']", namespaces).text
        for element in root.findall(".//structure:Code", namespaces)
    }


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the ILOSTAT SDMX API.
    """

    uri: HttpUrl = Field(default=BASE_URL, frozen=True, validate_default=True)

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the ILO SDMX API.

        Parameters
        ----------
        **kwargs
            Extra arguments to pass to `_get_data`.

        Returns
        -------
        pd.DataFrame
            Raw data from the API for the indicators with supported disaggregations.
        """

        df_metadata = self.get_metadata()
        # indicator codes contain disaggregations, e.g., SDG_0852_SEX_AGE_RT
        # subset only some disaggregations and no classification (NOC)
        mask = (
            df_metadata["code"]
            .str.split("_")
            .str.slice(2, -1)
            .apply(lambda x: not set(x) - DISAGGREGATIONS)
        )
        df_metadata = df_metadata.loc[mask].reset_index(drop=True)
        '''
        I believe some sources might use the storage (manual ones) but ILO does NOT need it
        
        '''
        storage = kwargs.pop('storage')

        data = []
        with self.client as client:
            for _, row in tqdm(df_metadata.iterrows(), total=len(df_metadata)):
                df = self._get_data(row.code, client=client, **kwargs)
                if df is None:
                    continue
                df["indicator_name"] = f"{row['name']} [{row['code']}]"
                data.append(df)
        return pd.concat(data, axis=0, ignore_index=True)

    def _get_metadata(self) -> pd.DataFrame:
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

    def _get_data(
        self,
        indicator_code: str,
        start_period: str = "2015-01-01",
        end_period: str = "2025-12-31",
        client: httpx.Client | None = None,
        **kwargs,
    ) -> pd.DataFrame | None:
        """
        Get indicator data from the ILO SDMX API data endpoint.

        Parameters
        ----------
        indicator_code : str
            Indicator code to retrieve data for. See `_get_metadata`.
        start_period : str, default="2015-01-01"
            Retrieve data from this date.
        end_period : str, default="2025-12-31"
            Retrieve data until this date.
        client : httpx.Client, optional
            Client to use for making an HTTP GET request.

        Returns
        -------
        pd.DataFrame or None
            Data frame with raw data as returned by the API or None.
        """
        params = {
            "format": "csvfile",
            "startPeriod": start_period,
            "endPeriod": end_period,
        } | kwargs
        return self.read_csv(f"data/ILO,{indicator_code}/", params, client)


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the ILOSTAT SDMX API.
    """

    def transform(self, df: pd.DataFrame, **kwargs):
        """
        Tranform function for raw ILO data.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data as returned by the retrieval section.

        Returns
        -------
        pd.DataFrame
            Standardised data frame.
        """

        columns = {
            "REF_AREA": "country_code",
            "indicator_name": "indicator_name",  # assigned by the retriever
            "SEX": "disagr_sex",
            "AGE": "disagr_age",
            "GEO": "disagr_geo",
            "EDU": "disagr_edu",
            "TIME_PERIOD": "year",
            "OBS_VALUE": "value",
            "OBS_STATUS": "prop_observation_type",
            "UNIT_MEASURE_TYPE": "unit",
        }

        # subset annual indicators
        df = df.query("FREQ == 'A'").copy()

        # keep only aggregate to avoid overlaps between aggregate, 5- and 10-year bands
        # and different classifications for education too
        for column in ("AGE", "EDU"):
            if column in df.columns:
                df = df.loc[df[column].str.contains("AGGREGATE", na=True)].copy()

        # replace disaggregation codes with labels
        mapping = {
            disaggregation: _get_codelist_mapping(disaggregation)
            for disaggregation in DISAGGREGATIONS
        }
        df = df.replace(mapping).infer_objects(copy=False)
        # remap measure types
        mapping = _get_codelist_mapping("UNIT_MEASURE")
        df["UNIT_MEASURE_TYPE"] = df["UNIT_MEASURE_TYPE"].map(mapping).fillna("Unknown")

        # reindex and rename columns
        df = df.reindex(columns=columns).rename(columns=columns)
        df.dropna(subset=["value"], inplace=True)
        return df
