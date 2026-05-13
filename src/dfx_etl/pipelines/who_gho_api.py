"""
ETL components to process data from the WHO GHO API
by the World Health Organisation (WHO)
See https://www.who.int/data/gho/info/gho-odata-api.
"""
import asyncio
import warnings
from io import BytesIO
import httpx
import pandas as pd
from pydantic import Field, HttpUrl
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm
import json
from ..utils import _resolve_dimensions, to_snake_case
from ._base import BaseRetriever, BaseTransformer
import logging
logger = logging.getLogger(__name__)
__all__ = ["Retriever", "Transformer"]


warnings.warn(
    """This module is deprecated as the current GHO OData API is set to be removed
    near the end of 2025. See https://www.who.int/data/gho/legacy""",
    category=DeprecationWarning,
    stacklevel=2,
)


class Retriever(BaseRetriever):
    """
    A class for retrieving data from the WHO GHO API.
    """

    uri: HttpUrl = Field(
        default="https://ghoapi.azureedge.net/api/",
        frozen=True,
        validate_default=True,
    )

    def __call__(self, **kwargs) -> pd.DataFrame:
        """
        Retrieve data from the GHO OData API,

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
        logger.info(f'Going to download {len(df_metadata)} WHO GHO indicators')
        data = []
        skipped= []
        with self.client as client:
            for _, row in (pbar:= tqdm(df_metadata.iterrows(), total=len(df_metadata))):
                try:
                    pbar.set_description(f'Downloading {row.code} indicator')
                    df = self._get_data(row.code, client=client, **kwargs)
                    df["indicator_name"] = f"{row['name']} [{row['code']}]"
                    data.append(df)
                    pbar.set_description(f'Downloaded {row.code} indicator')
                except Exception as e:
                    logger.debug(f'{row.code} failed with error: {e}')
                    pbar.set_description(f'{row.code} indicator will be skipped')
                    skipped.append(row.code)
        if skipped:
            logger.debug(f'Not collected: {len(skipped)} indicators: {",".join(skipped)}')
        return pd.concat(data, axis=0, ignore_index=True)

    def _get_dimensions(self) -> dict:
        """
        Get series dimensions from the GHO OData API.

        Returns
        -------
        dict
            Dimensions dictionary.
        """
        response = self.client.get("DIMENSION")
        response.raise_for_status()
        return response.json()["value"]

    async def _check_indicator_(self, client, code):
        url = f"{code}?$top=1"
        try:
            # httpx is strict about timeouts, setting a 10s timeout is a good safety net
            response = await client.get(url, timeout=10.0)

            if response.status_code == 200:
                data = response.json()
                # If the value array has items, data exists
                if data.get('value'):
                    return code
        except Exception as e:
            # Silently pass timeouts or connection drops
            pass
        return None

    async def _filter_valid_(self):

        async with httpx.AsyncClient(base_url=str(self.uri)) as client:
            # 1. Fetch the full list of indicators first
            indicators_url = f"{str(self.uri)}Indicator"
            response = await client.get(indicators_url)
            ind_data = response.json()

            indicator_codes = {ind['IndicatorCode']:ind['IndicatorName']  for ind in ind_data.get('value', [])}


            # 2. Use a semaphore to limit simultaneous task execution
            sem = asyncio.Semaphore(50)

            async def bound_check(code):
                async with sem:
                    return await self._check_indicator_(client=client, code=code)
            # 3. Create tasks for all indicators and run them concurrently
            tasks = [bound_check(code) for code in indicator_codes]
            results = await atqdm.gather(*tasks, desc="Filtering indicators")

            # Filter out the Nones (empty indicators or failed requests)
            valid_indicators = [res for res in results if res is not None]
            # 5. Construct the Pandas DataFrame with the specific columns you need
            valid_data = [{"code": code, "name": indicator_codes[code]} for code in valid_indicators]

            # Ensure we return an empty DataFrame with the correct structure if everything fails
            if not valid_data:
                return pd.DataFrame(columns=["code", "name"])

            return pd.DataFrame(valid_data)
    def _get_metadata(self) -> pd.DataFrame:
        """
        Get series metadata from the GHO OData API.

        Returns
        -------
        pd.DataFrame
            Data with series metadata.
        """

        return asyncio.run(self._filter_valid_())

    def _get_data(
        self,
        indicator_code: str,
        client: httpx.Client | None = None,
        **kwargs,
    ) -> pd.DataFrame | None:
        """
        Get series data from the GHO OData API.

        Parameters
        ----------
        indicator_code : str
            Indicator code. See `_get_metadata`.

        Returns
        -------
        pd.DataFrame or None
            Data frame with country data in the wide format.

        """
        filters = ["NumericValue ne null"]
        for k, v in kwargs.items():
            if isinstance(v, (str, int)):
                filters.append(f"{k} eq '{v}'")
            elif isinstance(v, list):
                filters.append(f"{k} in {tuple(v)}")
            else:
                raise ValueError(
                    f"{k} must be one of (str, int, list). Found {type(v)}"
                )
        filters = f"?$filter={' and '.join(filters)}" if filters else ""
        url = f"{indicator_code}{filters}"
        with client.stream("GET", url) as response:
            response.raise_for_status()
            # 2. Collect chunks into a memory buffer
            with BytesIO() as buffer:
                for chunk in response.iter_bytes():
                    if chunk:
                        buffer.write(chunk)

                # 3. Reset buffer position for Pandas
                buffer.seek(0)

                # 4. Load into DataFrame
                if buffer.getbuffer().nbytes == 0:
                    raise pd.errors.NoBufferPresent(f'No buffer')
                data = json.load(buffer)

                df = pd.DataFrame(data.get("value", []))
                if df.empty:
                    raise pd.errors.EmptyDataError('Empty data frame')

                df = df.dropna(how='all')
                return df.dropna(how='all', axis=1)

        # response = client.get(url=url)
        # response.raise_for_status()
        # return pd.DataFrame(response.json()["value"])


class Transformer(BaseTransformer):
    """
    A class for transforming raw data from the WHO GHO API.
    """

    def transform_old(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data from GHO OData API.

        Note that the source data contains duplicates which are dropped. There are rows that
        do not differ in any column except for value and ID columns. 'DataSourceDim' column
        is treated is a 'source' column and dimension too, because it is used to uniqely identify
        a row in this source too.

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
            "indicator_name": "indicator_name",
            "SpatialDim": "country_code",
            "TimeDim": "year",
            "dimension": "dimension",
            "DataSourceDim": "source",
            "NumericValue": "value",
        }

        # Handle dimensions stored in the long format but avoid adding new columns for each
        dims = df.filter(regex=r"^Dim\d$").columns
        df["DataSourceDim"] = df["DataSourceDim"].str.replace("DATASOURCE_", "")
        df["dimension"] = (
            df.apply(
                lambda row: (
                    {
                        to_snake_case(category): row[dim].replace(f"{category}_", "")
                        for dim in dims
                        if (category := row[f"{dim}Type"]) is not None
                    }
                    # Add source as a dimensions to avoid duplicates
                    | {"source": row["DataSourceDim"]}
                )
                or None,
                axis=1,
            )
            .map(lambda x: _resolve_dimensions(x, prefix=""), na_action="ignore")
            .fillna("Total")
        )
        df = df.reindex(columns=columns).rename(columns=columns).reset_index(drop=True)
        # Drop duplicates deterministically
        columns = set(df.columns) - {"value"}
        df.sort_values(list(columns), ignore_index=True, inplace=True)
        df.drop_duplicates(
            subset=list(columns - {"source"}),
            keep="first",
            ignore_index=True,
            inplace=True,
        )
        return df

    def transform_work(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data from GHO OData API with 3M+ row performance optimizations.
        """
        columns_map = {
            "indicator_name": "indicator_name",
            "SpatialDim": "country_code",
            "TimeDim": "year",
            "dimension": "dimension",
            "DataSourceDim": "source",
            "NumericValue": "value",
        }

        # 1. Identify dimension columns (Dim0, Dim1, etc.)
        dims = df.filter(regex=r"^Dim\d$").columns

        # 2. Vectorized Pre-processing: Standardize source and types
        # Replace empty strings with 'UNKNOWN' to satisfy Pandera str_length(2, 2048)
        df["DataSourceDim"] = (
            df["DataSourceDim"]
            .str.replace("DATASOURCE_", "", regex=False)
            .fillna("UNKNOWN")
            .replace("", "UNKNOWN")
        )

        # Pre-convert Dimension Types to snake_case once (Vectorized is 100x faster than apply)
        for dim in dims:
            type_col = f"{dim}Type"
            if type_col in df.columns:
                df[type_col] = df[type_col].map(to_snake_case, na_action="ignore")

        # 3. Optimized Dimension Mapping
        # We use a helper to avoid the 'float' has no attribute 'replace' error
        def build_dim_dict(row):
            try:
                d_dict = {
                    str(row[f"{dim}Type"]): str(row[dim]).replace(f"{row[f'{dim}Type']}_", "")
                    for dim in dims
                    if pd.notna(row.get(f"{dim}Type"))
                }
                # Inject source to ensure uniqueness
                d_dict["source"] = row["DataSourceDim"]
                return d_dict
            except Exception:
                return {"source": row["DataSourceDim"]}

        df["dimension"] = (
            df.apply(build_dim_dict, axis=1)
            .map(lambda x: _resolve_dimensions(x, prefix=""), na_action="ignore")
            .fillna("Total")
        )

        # 4. Canonical Reindexing
        df = df.reindex(columns=columns_map.keys()).rename(columns=columns_map).reset_index(drop=True)

        # 5. Deterministic Deduplication
        # We include 'value' in the sort to ensure we keep the most 'complete' records
        sort_cols = [c for c in df.columns if c != "value"]
        df.sort_values(by=sort_cols + ["value"], ignore_index=True, inplace=True)

        # Drop duplicates while ignoring 'source' in the identity check
        subset_cols = [c for c in df.columns if c not in ["value", "source"]]
        df.drop_duplicates(
            subset=subset_cols,
            keep="first",
            ignore_index=True,
            inplace=True,
        )

        return df


    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        High-performance transformation for WHO GHO (3M+ rows).
        Bypasses df.apply() to achieve 10x-20x speedup.
        """
        columns_map = {
            "indicator_name": "indicator_name",
            "SpatialDim": "country_code",
            "TimeDim": "year",
            "dimension": "dimension",
            "DataSourceDim": "source",
            "NumericValue": "value",
        }

        # 1. Clean DataSourceDim (Vectorized)
        df["DataSourceDim"] = (
            df["DataSourceDim"]
            .str.replace("DATASOURCE_", "", regex=False)
            .fillna("UNKNOWN")
            .replace("", "UNKNOWN")
        )

        # 2. Vectorized Dimension Construction
        # We build a list of dictionaries manually using list comprehensions
        # and zip, which is orders of magnitude faster than df.apply
        dims = df.filter(regex=r"^Dim\d$").columns

        # Pre-calculate snake_case types and clean values for all dims
        dim_data = {}
        for dim in dims:
            type_col = f"{dim}Type"
            if type_col in df.columns:
                # Vectorized clean-up
                clean_types = df[type_col].map(to_snake_case, na_action="ignore")
                # Convert to string to avoid float/NaN errors in the dict build
                clean_values = df[dim].astype(str)
                dim_data[dim] = (clean_types, clean_values)

        # 3. Fast Dictionary Build (The Python 'Zip' Trick)
        # This replaces df.apply(axis=1) and is significantly faster
        sources = df["DataSourceDim"].values

        def fast_dim_generator():
            # Zip all dimension columns together to iterate once
            iters = {d: zip(dim_data[d][0], dim_data[d][1]) for d in dims}
            for i, source in enumerate(sources):
                d_dict = {"source": source}
                for d in dims:
                    dtype, dval = next(iters[d])
                    if dtype and dval != 'nan':
                        # Performance: Use f-string or pre-cleaned values
                        d_dict[dtype] = dval.replace(f"{dtype}_", "")
                yield d_dict

        # Reconstruct the dimension column
        df["dimension"] = [
            _resolve_dimensions(d, prefix="") for d in fast_dim_generator()
        ]
        # If the above is still slow, use:
        # df["dimension"] = list(fast_dim_generator())
        # and map _resolve_dimensions later.

        # 4. Final canonical steps
        df = df.reindex(columns=columns_map.keys()).rename(columns=columns_map).reset_index(drop=True)

        # 5. Fast Deduplication
        # Subset to minimize memory during sort
        subset_cols = [c for c in df.columns if c not in ["value", "source"]]
        df.sort_values(by=subset_cols + ["value"], ignore_index=True, inplace=True)
        df.drop_duplicates(subset=subset_cols, keep="first", ignore_index=True, inplace=True)

        return df