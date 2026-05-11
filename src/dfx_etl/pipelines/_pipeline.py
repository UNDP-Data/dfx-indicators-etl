"""
The generic Pipeline class is generic to process any resource, given
the correct implementation of `retriever` and `transformer` components.
"""

import logging
import os.path
from inspect import signature
from typing import Self, final, Any
from dfx_etl.database import ignore_on_conflict, get_engine
import pandas as pd
from pydantic import BaseModel, ConfigDict, PrivateAttr

from ..settings import SETTINGS
from ..storage import BaseStorage, get_storage
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Pipeline"]

logger = logging.getLogger(__name__)


class Pipeline(BaseModel):
    """
    An ETL pipeline to process a single source.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    retriever: BaseRetriever
    transformer: BaseTransformer
    _storage: BaseStorage = PrivateAttr(default_factory=get_storage)
    _df_raw: pd.DataFrame | None = PrivateAttr(default=None)
    _df_transformed: pd.DataFrame | None = PrivateAttr(default=None)
    _df_loaded: pd.DataFrame | None = PrivateAttr(default=None)
    _engine: Any = PrivateAttr(default_factory=get_engine)

    def __call__(self) -> pd.DataFrame:
        """
        Run all steps of the ETL pipeline.

        Returns
        -------
        pd.DataFrame
            Validated data frame in the standard form.
        """
        self.retrieve()
        logger.info("Raw data shape: %s", self.df_raw.shape)
        self.transform()
        logger.info("Transformed data shape: %s", self.df_transformed.shape)
        self.load()
        logger.info("Loaded data shape: %s", self.df_loaded.shape)
        #return self.df_transformed

    @property
    def df_raw(self) -> pd.DataFrame:
        """
        Raw data as returned by the retriever.
        """
        return self._df_raw

    @property
    def df_transformed(self) -> pd.DataFrame:
        """
        Validated data as returned by the transformer.
        """
        return self._df_transformed

    @property
    def df_loaded(self) -> pd.DataFrame:
        """
        The ID-mapped data that mimics the database 'series' table.
        """
        return self._df_loaded

    @final
    def retrieve(self, **kwargs) -> Self:
        """
        Run the retrieval step to obtain raw data.

        Syntactic sugar that calls the underlying retriever.

        Parameters
        ----------
        **kwargs
            Keyword arguments to be passed to the retriever call.
        """
        # Pass a storage to the retriever only if it is expected
        if "storage" in signature(self.retriever).parameters:
            kwargs |= {"storage": self._storage}

        self._df_raw = self.retriever(**kwargs)
        self._df_raw.name = f'{self.retriever.provider}_raw'
        return self

    @final
    def transform(self, **kwargs) -> Self:
        """
        Run the transformation step on the raw data.

        Parameters
        ----------
        **kwargs
            Keyword arguments to be passed to the transformer call.
        """
        if self.df_raw is None:
            raise ValueError("No raw data. Run the retrieval first")
        df = self.transformer(
            self.df_raw.copy(), provider=self.retriever.provider, **kwargs
        )
        df = df.query(
            "year >= @year_min and year <= @year_max",
            local_dict={
                "year_min": SETTINGS.pipeline.year_min,
                "year_max": SETTINGS.pipeline.year_max,
            },
        ).reset_index(drop=True)
        df.name = f'{self.retriever.provider}_transformed'
        self._df_transformed = df
        return self

    @final
    def load(self):
        # --- PHASE 1: THE EXTRACTORS (Metadata) ---
        # These functions ensure the 'indicator' and 'dimension' tables are up to date.
        # We use your existing _extract functions here.
        self._sync_reference_data()

        # --- PHASE 2: THE INGESTOR (Mass Data) ---
        # Now that Stage 1 ensured all IDs exist, we stream the 11M rows.
        self._stream_series_data()
        self._df_loaded.name = f'{self.retriever.provider}_loaded'
        return self

    @final
    def persist(self, step: str = None, folder_path: str = None, frmt: str = 'parquet') -> str:
        """
        Serializes the pipeline steps to the selected storage and format.
        """
        if step:
            folder_path = folder_path or ''

            # Mapping logic for folder names
            step2folder = {
                'retrieve': 'raw',
                'transform': 'transformed',
                'load': 'series'  # Mimic the DB table name
            }

            step2df = {
                'retrieve': 'raw',
                'transform': 'transformed',
                'load': 'loaded'  # Mimic the DB table name
            }
            target_step = step2folder.get(step, step)
            folder_path = os.path.join(folder_path, target_step)

            # Identify which dataframe to grab
            attr_name = f'df_{step2df[step]}'
            df = getattr(self, attr_name)

            if df is None:
                logger.info(f"No data available to persist for step: {step}")
                return
            return self._storage.write_dataset(df, folder_path=folder_path, frmt=frmt)

    @final
    def _extract_indicators(self):
        columns = {"indicator_name": "name", "provider": "provider"}
        df_indicators = self.df_transformed.reindex(columns=columns).rename(columns=columns)
        df_indicators.drop_duplicates(ignore_index=True, inplace=True)
        df_indicators.sort_values(["provider", "name"], ignore_index=True, inplace=True)
        return df_indicators

    @final
    def _extract_dimensions(self):
        """
        Extract unique dimensions for the dimension table.
        """
        columns = {"dimension": "name"}
        df_dimensions = self.df_transformed.reindex(columns=columns).rename(columns=columns)
        df_dimensions.drop_duplicates(ignore_index=True, inplace=True)
        df_dimensions.sort_values(["name"], ignore_index=True, inplace=True)
        return df_dimensions

    def _sync_reference_data(self):
        """
        Ensures all indicator names and dimension labels exist in the DB
        before the mass ingestion starts.
        """


        # Sync Indicators
        self._extract_indicators().to_sql(
            "indicator",
            con=self._engine,
            #schema="dfx",
            if_exists="append",
            index=False,
            method=ignore_on_conflict
        )

        # Sync Dimensions
        self._extract_dimensions().to_sql(
            "dimension",
            con=self._engine,
            #schema="dfx",
            if_exists="append",
            index=False,
            method=ignore_on_conflict
        )

    def _stream_series_data(self):
        """
        Resolves IDs in-memory and streams the series data via PostgreSQL COPY.
        """
        raw_conn = self._engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                #cur.execute("SET search_path TO dfx")

                # 1. Retrieve the fresh ID Maps
                cur.execute("SELECT iso_3, id FROM country")
                cty_map = dict(cur.fetchall())
                cur.execute("SELECT name, id FROM indicator")
                ind_map = dict(cur.fetchall())
                cur.execute("SELECT name, id FROM dimension")
                dim_map = dict(cur.fetchall())

                # 2. In-Memory Translation
                df = self.df_transformed.copy()
                df['country_id'] = df['country_code'].map(cty_map)
                df['indicator_id'] = df['indicator_name'].map(ind_map)
                df['dimension_id'] = df['dimension'].map(dim_map)

                # 3. THE DROP LOG (Orphan Management)
                required_fks = ['country_id', 'indicator_id', 'dimension_id']
                initial_count = len(df)

                # Identify where mapping failed
                missing_mask = df[required_fks].isnull().any(axis=1)
                if missing_mask.any():
                    dropped_df = df[missing_mask]
                    logger.info(f"--- ORPHAN REPORT: {len(dropped_df)} rows dropped ---")
                    # Capture and sort unique culprits for a cleaner log output
                    missing_codes = sorted(dropped_df[dropped_df['country_id'].isna()]['country_code'].unique())
                    if missing_codes:
                        logger.warning(f"Missing Country Codes: {missing_codes}")

                    missing_inds = sorted(dropped_df[dropped_df['indicator_id'].isna()]['indicator_name'].unique())
                    if missing_inds:
                        logger.warning(f"Missing Indicators: {missing_inds}")

                # Filter to clean data
                df_clean = df.dropna(subset=required_fks).copy()
                for col in required_fks:
                    df_clean[col] = df_clean[col].astype(int)
                # --- THE PERSISTENCE HOOK ---
                # Store the DB-ready dataframe before streaming it
                self._df_loaded = df_clean[required_fks]
                # 4. Binary Stream (COPY Protocol)
                cur.execute("CREATE TEMP TABLE stage_series (LIKE series INCLUDING ALL) ON COMMIT DROP")

                cols = ['country_id', 'indicator_id', 'dimension_id', 'year', 'value']
                with cur.copy("COPY stage_series FROM STDIN") as copy:
                    for row in df_clean[cols].itertuples(index=False):
                        copy.write_row(row)

                # 5. Atomic Upsert
                cur.execute("""
                    INSERT INTO series (country_id, indicator_id, dimension_id, year, value)
                    SELECT * FROM stage_series
                    ON CONFLICT (country_id, indicator_id, dimension_id, year)
                    DO UPDATE SET value = EXCLUDED.value;
                """)

                logger.info(f"Ingestion Complete: {len(df_clean)} rows pushed ({len(df_clean) / initial_count:.1%})")

            raw_conn.commit()
        finally:
            raw_conn.close()





    @property
    def name(self):
        """
        Pipeline  name
        Returns str
        -------

        """
        try:
            return self.retriever.provider.split('_')[0].upper()
        except Exception:
            return self.retriever.provider.upper()
    def __str__(self):

        return f'Pipeline: {self.name}::{self.retriever.uri}->{self._storage}'