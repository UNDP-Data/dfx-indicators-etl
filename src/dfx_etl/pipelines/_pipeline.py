"""
The generic Pipeline class is generic to process any resource, given
the correct implementation of `retriever` and `transformer` components.
"""

import logging
from inspect import signature
from typing import Self, final

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
        return self.df_transformed

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
        df.name = self.retriever.provider
        self._df_transformed = df
        return self

    @final
    def load(self) -> Self:
        """
        Run the load step to write the transformed data to the storage.

        Returns
        -------
        str
            Full path to the file in the storage.
        """
        if self.df_transformed is None:
            raise ValueError("No validated data. Run the validation first")
        return self._storage.write_dataset(self.df_transformed)
