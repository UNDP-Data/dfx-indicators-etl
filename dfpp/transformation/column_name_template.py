"""store basic column name and column value conventions"""

from enum import StrEnum, Enum

CANONICAL_COLUMN_NAMES= ["alpha_3_code", "country_or_area", "year", "value", "series_id", "source"]


class SexEnum(StrEnum, Enum):
    MALE = "male"
    FEMALE = "female"
    BOTH = "both"
    OTHER = "other"
    TOTAL = "total"
    NOT_APPLICABLE = "not applicable"


def sort_columns_canonically(df):
    """
    Sort the columns of a DataFrame so that the canonical columns appear first, 
    in the specified order. Other columns remain in the DataFrame in their original order.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: A new DataFrame with the columns sorted.
    """
    assert all(col in df.columns for col in CANONICAL_COLUMN_NAMES), \
    f"DataFrame does not contain all canonical columns. Missing columns: {set(CANONICAL_COLUMN_NAMES) - set(df.columns)}"

    canonical_cols_start = [col for col in CANONICAL_COLUMN_NAMES if col in df.columns][:2]
    canonical_cols_end = [col for col in CANONICAL_COLUMN_NAMES if col in df.columns][2:]
    
    other_cols = [col for col in df.columns if col not in CANONICAL_COLUMN_NAMES]
    
    sorted_columns = canonical_cols_start + other_cols + canonical_cols_end
    
    return df[sorted_columns]