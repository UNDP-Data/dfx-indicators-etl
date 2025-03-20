"""undp specific geo transformations"""

import os
from io import BytesIO

import pandas as pd

from dfpp.storage import StorageManager

__all__ = [
    "get_iso3_to_official_name_map",
    "get_numeric_to_iso3_map",
    "get_string_to_iso3_map",
]


async def get_iso3_to_official_name_map() -> dict:
    "get a mapping of alpha_3_code codes to country official names"
    async with StorageManager() as storage_manager:
        path = os.path.join(storage_manager.utilities_path, "sep5_country_lookup.xlsx")
        data = await storage_manager.read_blob(path=path)
    df_country_map = pd.read_excel(BytesIO(data), sheet_name="country_lookup")
    iso_3_country = dict(df_country_map[["iso3", "countryname"]].values)
    return iso_3_country


async def get_numeric_to_iso3_map() -> dict:
    "get a mapping of numeric country codes to alpha_3_code codes"
    async with StorageManager() as storage_manager:
        path = os.path.join(storage_manager.utilities_path, "sep5_country_lookup.xlsx")
        data = await storage_manager.read_blob(path=path)
    df_country_map = pd.read_excel(BytesIO(data), sheet_name="country_lookup")
    df_region_map = pd.read_excel(BytesIO(data), sheet_name="region_lookup")

    df_country_map = df_country_map[df_country_map["Numeric code"].notna()]
    df_country_map["Numeric code"] = df_country_map["Numeric code"].astype(int)

    df_region_map = df_region_map[df_region_map["Numeric code"].notna()]
    df_region_map["Numeric code"] = df_region_map["Numeric code"].astype(int)

    iso_3_region = dict(df_region_map[["Numeric code", "Alpha-3 code"]].values)
    iso_3_region.pop(64)

    iso_3_country = dict(df_country_map[["Numeric code", "iso3"]].values)

    iso_3_country.update(iso_3_region)

    iso_3_map = iso_3_country
    return iso_3_map


async def get_string_to_iso3_map() -> dict:
    "get a mapping of string country codes to alpha_3_code codes"
    async with StorageManager() as storage_manager:
        path = os.path.join(storage_manager.utilities_path, "sep5_country_lookup.xlsx")
        data = await storage_manager.read_blob(path=path)
    df_country_map = pd.read_excel(BytesIO(data), sheet_name="country_lookup")
    iso_3_country = dict(df_country_map[["countryname", "iso3"]].values)
    return iso_3_country
