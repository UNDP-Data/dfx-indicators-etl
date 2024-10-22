import gzip
from io import BytesIO
import re

import pandas as pd

__all__ = [
    "extract_last_braket_string",
    "sanitize_category",
    "read_to_df_csv_indicator",
]


def extract_last_braket_string(text: str) -> str | None:
    """extract units from indicator label sting"""
    match = re.search(r"\(([^)]*)\)$", text)

    if match:
        return match.group(1).strip()

    return None


def sanitize_category(s):
    """sanitize column names"""
    s = s.split(":")[0]
    s = s.lower()
    s = re.sub(r"[()\[\]]", "", s)
    s = re.sub(r"[\s\W]+", "_", s)
    s = s.strip("_")
    return s


def read_to_df_csv_indicator(file: bytes) -> pd.DataFrame:
    """
    Reads a gzipped CSV file into a pandas DataFrame.
    """
    with gzip.GzipFile(fileobj=BytesIO(file)) as gz:
        df = pd.read_csv(gz, low_memory=False)
        return df
