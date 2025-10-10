"""
Pipelines and pipeline components for data sources.
"""

import logging
from typing import Annotated, Self, final

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, StringConstraints
from tqdm import tqdm

from ..storage import BaseStorage
from ..utils import get_country_metadata
from ..validation import schema
from ._base import BaseRetriever, BaseTransformer

__all__ = ["Pipeline"]

logger = logging.getLogger(__name__)


class Metadata(BaseModel):
    """
    Metadata properties of the pipeline.
    """

    name: str = Field(
        description="Short formal name of the source",
        examples=["ILO", "UNICEF"],
    )
    directory: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            to_lower=True,
            min_length=3,
            max_length=64,
            pattern="\w+",
        ),
    ] = Field(
        description="Short unique source name used as a directory name when publishing the data",
        examples=["ilo_org", "unicef_org"],
    )
    url: HttpUrl = Field(
        description="Full URL to the source website used to overwrite `source` column in the output data",
        examples=["https://ilostat.ilo.org", "https://sdmx.data.unicef.org"],
    )


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
    df_validated: pd.DataFrame | None = Field(default=None, repr=False)

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
        """
        self.retrieve()
        logger.info(f"Raw data shape: {self.df_raw.shape}")
        self.transform()
        logger.info(f"Transformed data shape: {self.df_transformed.shape}")
        self.validate()
        logger.info(f"Validated data shape: {self.df_validated.shape}")
        self.load()
        return self.df_validated

    @final
    def retrieve(self, **kwargs) -> Self:
        """
        Run the retrieval step to obtain raw data.
        """
        self.df_raw = self.retriever(**kwargs)
        return self.df_raw

    @final
    def transform(self, **kwargs) -> Self:
        """
        Run the transformation step of the raw data.
        """
        if self.df_raw is None:
            raise ValueError("No raw data. Run the retrieval first")
        df = self.transformer(self.df_raw, **kwargs)
        # ensure only areas from UN M49 are present
        country_codes = get_country_metadata("iso-alpha-3")
        df = df.loc[df["country_code"].isin(country_codes)].copy()
        # add source
        df["source"] = str(self.url)
        df.reset_index(drop=True, inplace=True)
        self.df_transformed = df
        return self

    @final
    def validate(self) -> Self:
        """
        Run the validation of the transformed data, coercing data types if applicable.
        """
        if self.df_transformed is None:
            raise ValueError("No transformed data. Run the transformation first")
        self.df_validated = schema.validate(self.df_transformed)
        return self

    @final
    def load(self) -> Self:
        """
        Run the load step to push the data to the storage.
        """
        if self.df_validated is None:
            raise ValueError("No validated data. Run the validation first")
        for indicator_code, df in tqdm(self.df_validated.groupby("indicator_code")):
            df.name = indicator_code
            self.storage.publish_dataset(df, folder_path=self.directory)
        return self
