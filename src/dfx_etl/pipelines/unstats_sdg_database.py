"""
ETL components to process data from the Global SDG Database by the UN Stats.
The pipeline is designed to work with manually exported Excel files in that the SDG API's
performance is lacking.

See https://unstats.un.org/sdgs/dataportal/database.
"""

import logging
import time
import timeit
from pathlib import Path

import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm
from zipfile import ZipFile
import os
import tempfile
from ..storage import BaseStorage
from ..utils import replace_country_metadata, to_snake_case
from ..validation import PREFIX_DIMENSION
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]

logger = logging.getLogger(__name__)


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the Global SDG database.

    Use bulk download to manually obtain the data first.
    """

    uri: HttpUrl = Field(
        default="https://dfxa.blob.core.windows.net/manual/UNSTATS*.zip",
        frozen=True,
        validate_default=True,
    )

    def __call__(self, storage: BaseStorage, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the WDI database files.

        Parameters
        ----------
        storage : BaseStorage
            Storage to retrieve the data file from.
        **kwargs
            Extra arguments to pass to `pd.read_*` function.

        Returns
        -------
        pd.DataFrame
            Raw data frame with the data from the databae.
        """
        resolved_uri = self.resolved_uri
        url = str(resolved_uri)
        _, arch_name = os.path.split(str(url))
        with tempfile.TemporaryDirectory() as tdir:
            with self.client.stream('GET', str(url)) as r:
                total = int(r.headers.get("Content-Length", 0))
                path = Path(tdir) / arch_name
                with open(path, "wb") as f, tqdm(desc=f"Downloading {str(url)}", total=total,
                                                 unit="B", unit_scale=True, leave=False, unit_divisor=1024) as pbar:
                    for chunk in r.iter_bytes():
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            data = []
            with ZipFile(path) as zf:
                members = [m for m in zf.namelist() if m.endswith(".xlsx")]
                for name in (pbar:= tqdm( sorted(members), desc=f'Processing {self.provider}')):
                    extracted_path = zf.extract(name, path=tdir)
                    df = pd.read_excel(extracted_path, engine='calamine', **kwargs)
                    data.append(df)
                    pbar.set_description(f'Processing {name}')
            return pd.concat(data, axis=0, ignore_index=True)




class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the Global SDG database.
    """

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform data from the WDI dataset.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data frame.

        Returns
        -------
        pd.DataFrame
            Transformed data frame in the canonical format.
        """
        # Non-dimension columns
        columns = {
            "Goal": None,
            "Target": None,
            "Indicator": None,
            "SeriesCode": "indicator_code",
            "SeriesDescription": "indicator_name",
            "GeoAreaCode": "country_code",
            "GeoAreaName": None,
            "TimePeriod": "year",
            "Value": "value",
            "Time_Detail": None,
            "TimeCoverage": None,
            "UpperBound": None,
            "LowerBound": None,
            "BasePeriod": None,
            "Source": "source",
            "GeoInfoUrl": None,
            "FootNote": None,
            "Nature": None,
            "Reporting Type": None,
            "Units": None,
        }
        # Infer diaggregation columns which differ depending on the SDG
        dimensions = list(set(df.columns) - set(columns))
        # Filter out the columns and create a mapping for renaming
        columns = {k: v for k, v in columns.items() if v is not None}
        columns |= {
            column: to_snake_case(column, prefix=PREFIX_DIMENSION)
            for column in dimensions
        }
        df = df.reindex(columns=columns).rename(columns=columns)
        df["indicator_name"] = df.apply(
            lambda row: f"{row.indicator_name} [{row.indicator_code}]", axis=1
        )
        df.drop(columns=["indicator_code"], inplace=True)
        df["country_code"] = replace_country_metadata(
            df["country_code"].astype(str), "m49", "iso-alpha-3"
        )
        # Handle values like '<2.5' or '>99' by keeping the numeric part only
        df["value"] = pd.to_numeric(
            df["value"].astype(str).str.lstrip("<|>"), errors="coerce"
        )
        df.dropna(subset=["value"], ignore_index=True, inplace=True)
        # Drop full duplicates since indicators may be repeated for several Goals
        df.drop_duplicates(ignore_index=True, inplace=True)
        return df
