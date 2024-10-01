"""undp specific geo transformations"""

import os
from io import BytesIO
import pandas as pd

from dfpp.storage import StorageManager


__all__ = ["get_iso3_map"]


async def get_iso3_map() -> dict:
    "get a mapping of numeric country codes to ISO3 codes"
    async with StorageManager() as storage_manager:
        path = os.path.join(storage_manager.utilities_path, "sep5_country_lookup.xlsx")
        data = await storage_manager.read_blob(path=path)
    df_country_map = pd.read_excel(BytesIO(data), sheet_name="country_lookup")
    df_country_map = df_country_map[df_country_map["Numeric code"].notna()]
    df_country_map["Numeric code"] = df_country_map["Numeric code"].astype(int)
    iso_3_country = dict(df_country_map[["iso3", "countryname"]].values)
    return iso_3_country
