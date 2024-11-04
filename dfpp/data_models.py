import ast
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)

__all__ = ["Source", "Indicator"]

common_model_config = ConfigDict(
    from_attributes=True,
    populate_by_name=True,
    arbitrary_types_allowed=True,
)


class BaseModelWithConfig(BaseModel):
    model_config = common_model_config

    @classmethod
    def flatten_dict_config(
        cls: BaseModel, config_dict: dict[str, Any]
    ) -> dict[str, Any]:
        """Unnest sections from config
        if the section is 'source' or 'indicator'."""
        config = {}
        for k, v in config_dict.items():
            if k not in ["source", "indicator"]:
                config[k] = v
            else:
                config.update(v)
        return config


class Source(BaseModelWithConfig):
    id: str = Field(
        ...,
        description="The unique identifier for the source.\
              This ID will be used to identify the source in the pipeline and should be unique.",
    )
    name: str | None = Field(
        None,
        description="The name of the source.\
              This name will be used to identify the source in the pipeline.\
                Note that some manual sources may not have a name.",
    )
    url: HttpUrl | str = Field(
        ...,
        description="The URL of the source or the file name if the source is local.",
    )
    frequency: Literal["Daily", "daily"] | None = Field(
        None,
        description="The frequency at which the source data is updated, such as Daily or daily.\
            Unused.",
    )
    source_type: Literal["Manual", "Auto"] = Field(
        ...,
        description="The type of the source.\
              This indicates the type of source data: csv, excel, json, or xml.",
    )
    save_as: str = Field(
        ..., description="The name of the file in which the source data will be saved."
    )
    file_format: Literal["csv", "excel", "json", "xml", "xlsx", "xls"] = Field(
        ...,
        description="The format of the file in which the source data will be saved, such as csv, excel, json, or xml.",
    )
    downloader_function: (
        Literal[
            "default_http_downloader",
            "cpia_downloader",
            "zip_content_downloader",
            "post_downloader",
            "vdem_downloader",
            "rcc_downloader",
            "sipri_downloader",
            "country_downloader",
            "get_downloader",
        ]
        | None
    ) = Field(
        None,
        description="The downloader function used to fetch the source data. \
            This function should be defined in the dfpp.retrieval module.",
    )
    country_iso3_column: str | None = Field(
        None, description="The name of the column that contains the ISO3 country codes."
    )
    country_name_column: str | None = Field(
        None,
        description="The name of the column that contains the names of the countries.",
    )
    datetime_column: str | None = Field(
        None,
        description="The name of the column that contains the date and time values.",
    )
    year: int | str | None = Field(
        ...,
        description="The name of the column that contains the year values.",
    )
    group_column: str | list | None = Field(
        ...,
        description="The name of the column or columns used to group data during transformation.\
              This can be a single column or in rare cases a list of columns.",
    )
    downloader_params: dict | None = Field(
        {},
        description="Additional parameters required by the downloader function for retrieving the source data.",
    )
    file_name: str | None = Field(
        None,
        description="The name of the file within a zip archive, if the source file is downloaded as a zip file.",
    )

    @model_validator(mode="after")
    def validate_url(self):
        source_type = self.source_type
        if not source_type:
            return self
        if source_type != "Manual":
            if not self.url:
                raise ValueError("URL must be of type HttpUrl if source is not Manual")
            if not self.downloader_function:
                raise ValueError("Auto source requires download function to be defined")
        return self

    @model_validator(mode="before")
    def literal_eval_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        for field, value in values.items():
            try:
                values[field] = ast.literal_eval(value)
            except Exception:
                values[field] = value
        return values


class Indicator(BaseModelWithConfig):
    indicator_id: str = Field(
        ...,
        description="The unique identifier for the indicator.\
                               This ID will be used to identify the indicator in the pipeline.",
    )
    indicator_name: str = Field(
        ...,
        description="The full name of the indicator.\
                                 This name will be used to identify the indicator in the pipeline.",
    )
    display_name: str = Field(
        ...,
        description="The display name of the indicator.\
              This name will be used to display the indicator in the Data Futures Platform.",
    )
    source_id: str = Field(
        ...,
        description="The unique identifier of the data source.\
              This ID must be unique and will be used to identify the source in the pipeline.",
    )
    data_type: Literal["float", "text", "string"] = Field(
        ...,
        description="The data type of the indicator.\
              Can be 'float', 'text', or 'string' depending on the column(s) data type in the source file.",
    )
    frequency: Literal["Daily"] | None = Field(
        None,
        description="The frequency at which the indicator data is updated, such as 'Daily'.",
    )
    aggregate_type: Literal["PositiveSum", "Sum"] | None = Field(
        None,
        description="The type of aggregation applied to the indicator data.\
              Possible values include 'PositiveSum' and 'Sum'.",
    )
    preprocessing: str = Field(
        ...,
        description="The preprocessing function applied to the data before transformation.\
              This function should be defined in the dfpp.preprocessing module.",
    )
    source_field_name: str | tuple | None = Field(
        None,
        description="The original name of the field in the source data.\
          Note: Currently unused, apart from one indicator preprocessing function.",
    )
    transform_function: Literal[
        "type1_transform", "type2_transform", "type3_transform", "sme_transform"
    ] = Field(
        ...,
        description="The transformation function applied to the data.\
               This function should be defined in the dfpp.transform_functions module.",
    )
    group_name: str | tuple | None = Field(
        None,
        description="The group name associated with the indicator data. \
            Should be validated against group column from indicators.",
    )
    year: int | str | None = Field(
        None,
        description="The year for which the indicator data is relevant.\
              If the indicator covers multiple years, this field should be set to None.",
    )
    value_column: str | None = Field(
        None,
        description="The name of the column that contains the values of the indicator.",
    )
    column_substring: str | None = Field(
        None,
        description="The substring used to identify specific columns that contain values related to the indicator.",
    )
    sheet_name: str | None = Field(
        None,
        description="The name of the sheet in the data file where the indicator data is located.\
             Note: this is only used when 'mpi_transform_preprocessing' preprocessing is applied.",
    )
    denominator_indicator_id: (
        Literal[
            "totalpopulation_untp",
            "totalruralpopulation_cpiatrp",
            "totalurbanpopulation_cpiatup",
            "cumulativecases_whoglobal",
        ]
        | None
    ) = Field(
        None,
        description="The ID of the indicator used as a denominator in calculations.",
    )
    per_capita: float | None = Field(
        None, description="Multiplier for per capita calculations."
    )
    min_year: float | None = Field(
        None, description="Minimum year for which the indicator data is relevant."
    )
    filter_sex_column: str | None = Field(None)
    filter_frequency_column: str | None = Field(None)
    filter_age_column: str | None = Field(None)
    filter_ste_column: str | None = Field(None)
    filter_eco_column: str | None = Field(None)
    filter_value_column: str | None = Field(None)
    value_col: str | None = Field(None)
    divisor: str | list | None = Field(None)
    dividend: str | None = Field(None)

    @model_validator(mode="before")
    def literal_eval_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        for field, value in values.items():
            try:
                new_value = ast.literal_eval(value)
                if (
                    field in ["source_field_name", "value_column"]
                    and isinstance(new_value, float) == True
                ):
                    values[field] = value
                else:
                    values[field] = new_value
            except Exception:
                values[field] = value
        return values

    @field_validator("preprocessing")
    def is_preprocessing_implemented(cls, value):
        from dfpp.transformation import preprocessing

        if hasattr(preprocessing, value):
            return value
        raise NotImplementedError(
            f"The preprocessing function {value} \
                                  is not found or has not been not implemented"
        )
