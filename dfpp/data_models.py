from typing import Optional, Literal, Union, Any
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    root_validator,
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
    id: str = Field(..., description="Source Id.")
    name: Optional[str] = Field(None, desciption="Few manual sources have no name")
    url: HttpUrl | str = Field(..., description="URL or file name.")
    frequency: Optional[Literal["Daily", "daily"]]
    source_type: Literal["Auto", "Manual"]
    save_as: str = Field(..., description="Store file as defined string name.")
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
        ]
    ]
    country_iso3_column: Optional[str]
    country_name_column: Optional[str]
    datetime_column: Optional[str]
    year: Optional[int | str | None] = Field(..., description="Year, unused variable")
    group_column: Optional[str | list] = Field(
        ..., description="One or multiple columns to group by during transformation."
    )
    downloader_params: Optional[dict] = Field(
        {},
        description="Additional parameters to use during download of the raw source data.",
    )

    @field_validator("frequency")
    def normalize_frequency(cls, value):
        if value:
            return value.capitalize()
        return None

    @root_validator(pre=False, skip_on_failure=True)
    def validate_url(cls, values):
        source_type = values.get("source_type")
        if not source_type:
            return values
        if source_type != "Manual":
            if not values["url"]:
                raise ValueError("URL must be of type HttpUrl if source is not Manual")
        return values

    @root_validator(pre=True)
    def literal_eval_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        for field, value in values.items():
            try:
                values[field] = ast.literal_eval(value)
            except Exception as e:
                values[field] = value
        return values


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
    frequency: Optional[Literal["Daily"]] = Field(
        None, description="The frequency at which the data is updated."
    )
    # TBD: Only one config has non-empty field, modify and remove the field
    # aggregatetype: Optional[str] = Field(
    #    None, description="Type of aggregation applied, if any.")
    preprocessing: Optional[str] = Field(
        None, description="Preprocessing function applied to the data."
    )

    source_field_name: Optional[str | tuple] = Field(
        None,
        description="Original name of the field in the source data. #Currently unused",
    )
    transform_function: Literal[
        "type1_transform", "type2_transform", "type3_transform", "sme_transform"
    ] = Field(..., description="Transformation function applied to the data.")
    group_name: Optional[str | tuple] = Field(
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
        None,
        description="Sheet name in the data file, if applicable. Only used in mpi_transform_preprocessing preprocessing",
    )
    aggregate_type: Optional[Literal["PositiveSum", "Sum"]] = Field(
        None, description="Type of aggregation if applied."
    )
    denominator_indicator_id: Optional[
        Literal[
            "totalpopulation_untp",
            "totalruralpopulation_cpiatrp",
            "totalurbanpopulation_cpiatup",
            "cumulativecases_whoglobal",
        ]
    ] = Field(None, description="ID of the indicator used as a denominator.")
    per_capita: Optional[float] = Field(None, description="Per capita multiplier.")
    min_year: Optional[float] = Field(None, description="Minimum year for the data.")

    @root_validator(pre=True)
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
            except Exception as e:
                values[field] = value
        return values

    @root_validator(pre=False, skip_on_failure=True)
    def validate_group_name_to_group_column(cls, values):
        group_name = values.get("group_name")
        group_column = values.get("group_column")
        if (
            isinstance(group_name, str) and isinstance(group_column, (tuple, list))
        ) or (isinstance(group_name, (tuple, list)) and isinstance(group_column, str)):
            raise ValueError(
                "If 'group_name' or 'group_column' is a string, the other cannot be a tuple."
            )
        if isinstance(
            group_name,
            (
                list,
                tuple,
            ),
        ) and isinstance(group_column, (list, tuple)):
            if len(group_name) != len(group_column):
                raise ValueError(
                    "The length of 'group_name' must match the length of 'group_column',\
                                 check that length of the group_name index is equal to the group_column length."
                )


        return values
