from typing import Optional, Literal, Union
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    root_validator,
)


common_model_config = ConfigDict(
    from_attributes=True,
    populate_by_name=True,
    arbitrary_types_allowed=True,
)


class BaseModelWithConfig(BaseModel):
    model_config = common_model_config

    @field_validator("year", check_fields=False)
    def validate_year(cls, value):
        if value and value != "None":
            try:
                int(value)
            except ValueError:
                raise ValueError("year must be an integer or None") from None
        return value

    @root_validator(pre=False, skip_on_failure=True)
    def set_none_if_none(cls, values):
        for field_name, value in values.items():
            if value == "None":
                values[field_name] = None
        return values


class Source(BaseModelWithConfig):
    id: str
    name: str
    url: HttpUrl
    frequency: Optional[Literal["Daily", "None", "daily"]]
    source_type: Literal["Auto", "Manual"]
    save_as: str
    file_format: Literal["csv", "xlsx", "json", "xls"]
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
            "None",
        ]
    ]
    country_iso3_column: Optional[str]
    country_name_column: Optional[str]
    datetime_column: Optional[str]
    year: Optional[str]
    group_column: Optional[str]

    @field_validator("frequency")
    def normalize_frequency(cls, value):
        if value:
            return value.capitalize()
        return None


class Indicator(BaseModelWithConfig):
    indicator_id: str = Field(..., description="A unique identifier for the indicator.")
    indicator_name: str = Field(..., description="The full name of the indicator.")
    display_name: str = Field(
        ..., description="The name to be displayed for the indicator."
    )
    source_id: str = Field(..., description="The identifier of the data source.")
    data_type: Literal["float", "text", "string"] = Field(
        ..., description="The data type of the indicator."
    )
    # TBD: Duplicate, remove, frequency should be set by source
    frequency: Literal["Daily", "None"] = Field(
        ..., description="The frequency at which the data is updated."
    )
    # TBD: Only one config has non-empty field, modify and remove the field
    # aggregatetype: Optional[str] = Field(
    #    None, description="Type of aggregation applied, if any."
    # )
    preprocessing: Optional[str] = Field(
        None, description="Preprocessing function applied to the data."
    )
    source_field_name: Optional[str] = Field(
        None, description="Original name of the field in the source data."
    )
    transform_function: Literal[
        "type1_transform", "type2_transform", "type3_transform", "sme_transform"
    ] = Field(..., description="Transformation function applied to the data.")
    group_name: Optional[str] = Field(
        None, description="Group name associated with the data."
    )
    year: Optional[Union[int, str]] = Field(
        None, description="The year for which the data is relevant."
    )
    value_column: Optional[str] = Field(
        None, description="Name of the value column in the source data."
    )
    column_substring: Optional[str] = Field(
        None, description="Substring to identify specific columns."
    )
    sheet_name: Optional[str] = Field(
        None, description="Sheet name in the data file, if applicable."
    )
    aggregate_type: Optional[Literal["None", "PositiveSum", "Sum"]] = Field(
        None, description="Type of aggregation if applied."
    )
    denominator_indicator_id: Literal[
        "totalpopulation_untp",
        "totalruralpopulation_cpiatrp",
        "totalurbanpopulation_cpiatup",
        "cumulativecases_whoglobal",
    ] = Field(None, description="ID of the indicator used as a denominator.")
    per_capita: Optional[float] = Field(None, description="Per capita multiplier.")
    min_year: Optional[float] = Field(None, description="Minimum year for the data.")
