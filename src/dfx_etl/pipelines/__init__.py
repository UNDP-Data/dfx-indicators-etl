"""
Pipelines and pipeline components for data sources.

Source-specific processing logic is defined in their respective submodules.
The `Pipeline` class is generic and can be used to process any resource, given
the correct implementation of `retriever` and `transformer` components.
"""

import logging
from typing import Self, final
from urllib.parse import urlparse

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, computed_field

from ..storage import BaseStorage
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Pipeline"]

logger = logging.getLogger(__name__)


class Metadata(BaseModel):
    """
    Metadata properties of the pipeline.
    """

    url: HttpUrl = Field(
        description="URL to the website used to overwrite `source` column in the output data",
        examples=["https://ilostat.ilo.org", "https://sdmx.data.unicef.org"],
    )

    @computed_field
    @property
    def name(self) -> str:
        """
        A standardised name based on the source URL.

        The name is used as file name when saving data.
        """
        netloc = urlparse(str(self.url)).netloc
        return netloc.lower().replace(".", "-")


class Pipeline(Metadata):
    """
    An ETL pipeline to process a single source.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    retriever: BaseRetriever
    transformer: BaseTransformer
    storage: BaseStorage = Field(repr=False)
    df_raw: pd.DataFrame | None = Field(default=None, repr=False)
    df_transformed: pd.DataFrame | None = Field(default=None, repr=False)

    def __repr__(self) -> str:
        """
        Overwrite the represetnation to avoid data frame clutter.
        """
        string = super().__repr__()[:-1]  # strip the last `)`
        for k, v in self.model_dump().items():
            # for data frames, only show the shape
            if k.startswith("df_"):
                if v is not None:
                    v = f"<DataFrame shape={v.shape}>"
                string += f", {k}={v}"
        string += ")"
        return string

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
        self.df_raw = self.retriever(**kwargs)
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
        df = self.transformer(self.df_raw, **kwargs)
        # add source
        df["source"] = str(self.url)
        df.reset_index(drop=True, inplace=True)
        df.name = self.name
        self.df_transformed = df
        return self

    @final
    def load(self) -> Self:
        """
        Run the load step to push the transformed data to the storage.

        The function writes one parquet file per indicator code, using the code
        as a file name.
        """
        if self.df_transformed is None:
            raise ValueError("No validated data. Run the validation first")
        self.storage.publish_dataset(self.df_transformed)
        return self
