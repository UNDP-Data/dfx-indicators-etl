"""
Utility functions for reading axillary data distributed with the package and
performing minor data munging routines.
"""

import re
from importlib import resources
from io import StringIO
from typing import Literal, Sequence, TypeAlias

import pandas as pd

from . import data

__all__ = [
    "read_data_text",
    "read_data_binary",
    "read_data_csv",
    "get_country_metadata",
    "replace_country_metadata",
    "to_snake_case",
    "_combine_disaggregations",
]

CountryField: TypeAlias = Literal["name", "m49", "iso-alpha-2", "iso-alpha-3"]


def read_data_text(file_name: str) -> str:
    """
    Read a text file from the package's `data` directory.

    Parameters
    ----------
    file_name : str
        Name of the file to read.

    Returns
    -------
    str
        Contents of the file as a string.
    """
    with resources.open_text(data, file_name) as file:
        return file.read()


def read_data_binary(file_name: str) -> bytes:
    """
    Read a binary file from the package's `data` directory.

    Parameters
    ----------
    file_name : str
        Name of the file to read.

    Returns
    -------
    bytes
        Contents of the file as bytes.
    """
    with resources.open_binary(data, file_name) as file:
        return file.read()


def read_data_csv(file_name: str, **kwargs) -> pd.DataFrame:
    """
    Read a CSV file from the package's `data` directory.

    Parameters
    ----------
    file_name : str
        Name of the file to read.
    **kwargs
        Additional keywords arguments to pass to `pd.read_csv`.

    Returns
    -------
    pd.DataFrame
        Pandas data frame with the contents of the CSV file.
    """
    content = read_data_text(file_name)
    return pd.read_csv(StringIO(content), **kwargs)


def get_country_metadata(
    field: CountryField = "iso-alpha-3", sort: bool = True
) -> list[str]:
    """
    Get a country metadata field, such as names or codes.

    Parameters
    ----------
    field : CountryField, default='iso-alpha-3'
        Name of the metadata field.
    sort : bool, default=True
        If True, sort the values.

    Returns
    -------
    list[str]
        List of metadata values as they appear in UNSD M49.
    """
    mapping = {
        "name": "Country or Area",
        "m49": "M49 Code",
        "iso-alpha-2": "ISO-alpha2 Code",
        "iso-alpha-3": "ISO-alpha3 Code",
    }
    column = mapping[field]
    # Avoid reading Namibia's ISO code ('NA') as NaN
    df = read_data_csv("unsd-m49.csv", sep=";", keep_default_na=False)
    values = df[column].astype("str").tolist()
    if sort:
        values.sort()
    return values


def replace_country_metadata(
    values: Sequence[str | None],
    source: CountryField,
    target: CountryField,
) -> list[str | None]:
    """
    Replace country metadata field values with values from another field.

    This function can be used to map ISO 3166-1 alpha-2 to alpha-3 codes
    or alpha-3 codes to UNSD area names, among other things.

    Parameters
    ----------
    values : Sequence[str | None]
        Sequence of values to replace.
    source : CountryField
        Name of the field the values correspond to.
    target : CountryField
        Name of the field the values should be mapped to.

    Returns
    -------
    list[str | None]
        List of target metadata values.

    Examples
    --------
    >>> replace_country_metadata(["DZA", None, "AUT", "usa"], "iso-alpha-3", "name")
    ['Algeria', None, 'Austria', None]

    The values are case-sensitive. Any non-matching value is replaced with None.
    """
    mapping = dict(
        zip(
            get_country_metadata(source, sort=False),
            get_country_metadata(target, sort=False),
        )
    )
    return [mapping.get(value) for value in values]


def to_snake_case(value: str, prefix: str = "", suffix: str = "") -> str:
    """
    Convert a string value to snake case, optionally adding a prefix and/or suffix.

    Parameters
    ----------
    value : str
        String to be converted to snake case.
    prefix : str, optional
        String value to add as a prefix.
    suffix : str, optional
        String value to add as a suffix.

    Returns
    -------
    str
        Input value in snake case with the prefix and/or suffix if applicable.

    Examples
    --------
    >>> to_snake_case("Time Period")
    'time_period'
    >>> to_snake_case(" Time\n\n\nPeriod  ", prefix="dim", suffix="years")
    'dim_time_period_years'
    """
    value = re.sub(r"\s+", "_", value.strip().lower())
    if prefix:
        value = f"{prefix}_{value}"
    if suffix:
        value = f"{value}_{suffix}"
    return value


def _resolve_disaggregations(mapping: pd.Series | dict, prefix: str) -> str:
    """
    Combine disaggregations into a single value.

    Parameters
    ----------
    mapping : pd.Series or dict
        Series or dictionary with disaggregation values as values and fields as indexes or keys.
    prefix : str
        Prefix used for disaggregation fields to be removed.

    Returns
    -------
    str
        A single disaggregation value.
    """
    mapping = {
        name.replace(prefix, "", 1).replace("_", " "): value
        for name, value in mapping.items()
        if not pd.isna(value)
    }
    values = [
        value if value.lower() != "total" else f"All {name}"
        for name, value in mapping.items()
    ]
    if not values:
        return "Total"
    return "; ".join(values)


def _combine_disaggregations(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Combine disaggregations columns into a single column.

    This function is used as a parser during validation.

    Parameters
    ----------
    df : pd.DataFrame
        Input data frame passed to the validation schema.
    prefix : str
        Prefix used to identify disaggregation columns.

    Returns
    -------
    df : pd.DataFrame
        The data frame with disaggregation columns combined into one.
    """
    if "disaggregation" in df.columns:
        return df
    columns = [column for column in df.columns if column.startswith(prefix)]
    if not columns:
        return df.assign(disaggregation="Total")
    return df.assign(
        disaggregation=df[columns].apply(
            lambda row: _resolve_disaggregations(row, prefix), axis=1
        )
    )
