"""
ETL components to process data from the UNICEF SDMX API
by United Nations Children's Fund (UNICEF).
See https://data.unicef.org/sdmx-api-documentation/ and
https://sdmx.data.unicef.org/overview.html.
"""

import httpx
import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm

from ..validation import PREFIX_DIMENSION
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Retriever", "Transformer"]


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the UNICEF SDMX API.
    """

    uri: HttpUrl = Field(
        default="https://sdmx.data.unicef.org/ws/public/sdmxapi/rest",
        frozen=True,
        validate_default=True,
    )
    dataflow: str = Field(
        default="UNICEF,GLOBAL_DATAFLOW,1.0",
        frozen=True,
        validate_default=True,
        description="Dataflow value as it should appear in the query.",
    )

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the UNICEF SDMX API,

        Parameters
        ----------
        **kwargs
            Extra arguments to pass to `_get_data`.

        Returns
        -------
        pd.DataFrame
            Raw data from the API for the indicators with supported disaggregations.
        """
        df_metadata = self._get_metadata()
        fields = self._get_query_fields()
        data = []
        with self.client as client:
            for _, row in tqdm(df_metadata.iterrows(), total=len(df_metadata)):
                df = self._get_data(row.code, fields, client=client, **kwargs)
                if df is None:
                    continue
                data.append(df)
        return pd.concat(data, axis=0, ignore_index=True)

    def _get_dataflow(self) -> dict:
        params = {
            "format": "fusion-json",
            "dimensionAtObservation": "AllDimensions",
            "detail": "structureOnly",
            "includeMetrics": True,
            "includeMetadata": True,
            "match": "all",
            "includeAllAnnotations": True,
        }
        response = self.client.get(f"data/{self.dataflow}", params=params)
        response.raise_for_status()
        return response.json()

    def _get_query_fields(self) -> list[str]:
        data = self._get_dataflow()
        observation = data["structure"]["dimensions"]["observation"]
        return [x["id"].lower() for x in observation]

    def _set_query_options(self, fields: list[str], **kwargs) -> list[str]:
        if set(fields) & set(kwargs):
            values = []
            for option in fields:
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
        return options

    def _get_metadata(self) -> pd.DataFrame:
        """
        Get series metadata from UNICEF Indicator Data Warehouse.

        Returns
        -------
        pd.DataFrame
            Data frame with metadata columns.
        """
        columns = {"id": "code", "name": "name"}
        data = self._get_dataflow()
        observation = data["structure"]["dimensions"]["observation"]
        indicators = [x for x in observation if x["id"] == "INDICATOR"][0]["values"]
        indicators = [indicator for indicator in indicators if indicator["inDataset"]]
        return pd.DataFrame(indicators).reindex(columns=columns).rename(columns=columns)

    def _get_data(
        self,
        indicator_code: str,
        fields: list[str] | None = None,
        client: httpx.Client | None = None,
        **kwargs,
    ) -> pd.DataFrame | None:
        """
        Get series data from UNICEF Indicator Data Warehouse.

        Parameters
        ----------
        indicator_code : str
            Indicator code to retrieve data for.
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
        >>> _get_data(
        ...    "UNICEF,GLOBAL_DATAFLOW,1.0",
        ...    indicator_code="DM_POP_TOT",
        ...    time_period=["2020", "2021"],
        ... )
        #       REF_AREA Geographic area INDICATOR  ... Current age
        # 0     AFG      Afghanistan	 DM_POP_TOT ... Total
        # ...
        # 69041 ZWE      Zimbabwe        DM_POP_TOT ... Total
        """
        if fields is None:
            fields = self._get_query_fields()
        options = self._set_query_options(fields, indicator=indicator_code, **kwargs)
        params = {"format": "csv", "labels": "both"}
        return self.read_csv(f"data/{self.dataflow}/{options}", params, client)


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the UNICEF SDMX API.
    """

    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data from UNICEF SDMX API.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data frame.

        Returns
        -------
        pd.DataFrame
            Transformed data frame in the canonical format.
        """
        columns = {
            "REF_AREA": "country_code",
            "indicator_name": "indicator_name",
            "Sex": f"{PREFIX_DIMENSION}sex",
            "Current age": f"{PREFIX_DIMENSION}age",
            "TIME_PERIOD": "year",
            "OBS_VALUE": "value",
            "DATA_SOURCE": "source",
        }
        # subset yearly data
        df = df.loc[
            df["TIME_PERIOD"].astype(str).str.strip().str.match(r"^\d{4}$")
        ].copy()
        # handle values like <1 or <100 or >95%
        # the values now represent and upper/lower bound respectively
        df["OBS_VALUE"] = df["OBS_VALUE"].apply(
            lambda x: x.strip("<>") if isinstance(x, str) else x
        )
        df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
        df.dropna(subset=["OBS_VALUE"], inplace=True)
        df["indicator_name"] = df.apply(
            lambda row: f"{row['Indicator']}, {row['Unit of measure']} [{row['INDICATOR']}]",
            axis=1,
        )
        df["DATA_SOURCE"] = df["DATA_SOURCE"].combine_first(df["SOURCE_LINK"])
        return df.reindex(columns=columns).rename(columns=columns)
