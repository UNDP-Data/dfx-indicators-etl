from typing import Optional, Literal, Union, Any
from pydantic import (
    BaseModel,
    ValidationInfo,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    model_validator,

)
import ast

__all__ = ["Source", "Indicator"]

common_model_config = ConfigDict(
    from_attributes=True,
    populate_by_name=True,
    arbitrary_types_allowed=True,
)


class BaseModelWithConfig(BaseModel):
    model_config = common_model_config


class Source(BaseModelWithConfig):
    id: str = Field(
        ...,
        description="The unique identifier for the source.\
              This ID will be used to identify the source in the pipeline and should be unique.",
    )
    name: Optional[str] = Field(
        None,
        description="The name of the source.\
              This name will be used to identify the source in the pipeline.\
                Note that some manual sources may not have a name.",
    )
    url: HttpUrl | str = Field(
        ...,
        description="The URL of the source or the file name if the source is local.",
    )
    frequency: Optional[Literal["Daily", "daily"]] = Field(
        None,
        description="The frequency at which the source data is updated, such as Daily or daily.",
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
    downloader_function: Optional[
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
    ] = Field(
        None,
        description="The downloader function used to fetch the source data. \
            This function should be defined in the dfpp.retrieval module.",
    )
    country_iso3_column: Optional[str] = Field(
        None, description="The name of the column that contains the ISO3 country codes."
    )
    country_name_column: Optional[str] = Field(
        None,
        description="The name of the column that contains the names of the countries.",
    )
    datetime_column: Optional[str] = Field(
        None,
        description="The name of the column that contains the date and time values.",
    )
    year: Optional[int | str | None] = Field(
        ...,
        description="The name of the column that contains the year values.",
    )
    group_column: Optional[str | list] = Field(
        ...,
        description="The name of the column or columns used to group data during transformation.\
              This can be a single column or in rare cases a list of columns.",
    )
    downloader_params: Optional[dict] = Field(
        {},
        description="Additional parameters required by the downloader function for retrieving the source data.",
    )
    file_name: Optional[str] = Field(
        None,
        description="The name of the file within a zip archive, if the source file is downloaded as a zip file.",
    )

    @field_validator("frequency")
    def normalize_frequency(cls, value):
        if value:
            return value.capitalize()
        return None

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
    frequency: Optional[Literal["Daily"]] = Field(
        None,
        description="The frequency at which the indicator data is updated, such as 'Daily'.",
    )
    aggregate_type: Optional[Literal["PositiveSum", "Sum"]] = Field(
        None,
        description="The type of aggregation applied to the indicator data.\
              Possible values include 'PositiveSum' and 'Sum'.",
    )
    preprocessing: str = Field(
        ...,
        description="The preprocessing function applied to the data before transformation.\
              This function should be defined in the dfpp.preprocessing module.",
    )
    source_field_name: Optional[str | tuple] = Field(
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
    group_name: Optional[str | tuple] = Field(
        None,
        description="The group name associated with the indicator data. \
            Should be validated against group column from indicators.",
    )
    year: Optional[Union[int, str]] = Field(
        None,
        description="The year for which the indicator data is relevant.\
              If the indicator covers multiple years, this field should be set to None.",
    )
    value_column: Optional[str] = Field(
        None,
        description="The name of the column that contains the values of the indicator.",
    )
    column_substring: Optional[str] = Field(
        None,
        description="The substring used to identify specific columns that contain values related to the indicator.",
    )
    sheet_name: Optional[str] = Field(
        None,
        description="The name of the sheet in the data file where the indicator data is located.\
             Note: this is only used when 'mpi_transform_preprocessing' preprocessing is applied.",
    )
    denominator_indicator_id: Optional[
        Literal[
            "totalpopulation_untp",
            "totalruralpopulation_cpiatrp",
            "totalurbanpopulation_cpiatup",
            "cumulativecases_whoglobal",
        ]
    ] = Field(
        None,
        description="The ID of the indicator used as a denominator in calculations.",
    )
    per_capita: Optional[float] = Field(
        None, description="Multiplier for per capita calculations."
    )
    min_year: Optional[float] = Field(
        None, description="Minimum year for which the indicator data is relevant."
    )

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
        raise NotImplementedError(f"The preprocessing function {value} \
                                  is not found or has not been not implemented")