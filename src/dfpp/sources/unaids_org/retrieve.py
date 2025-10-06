"""
Functions to retrieve data from UNAIDS Key Population Atlas.
See https://kpatlas.unaids.org
"""

from urllib.parse import urljoin

import pandas as pd

__all__ = ["get_series_data"]

BASE_URL = "https://aidsinfo.unaids.org/public/documents/"


def get_series_data(series_id: str = "KPAtlasDB_2025_en.zip", **kwargs) -> pd.DataFrame:
    """
    Get series data from UNAIDS Key Population Atlas.

    Parameters
    ----------
    series_id : str
        ID of the series file.
    **kwargs
        Keywords arguments passed to `pd.read_csv`.

    Returns
    -------
    pd.DataFrame
        Data frame with data from the dashboard.
    """
    return pd.read_csv(urljoin(BASE_URL, series_id), **kwargs)
