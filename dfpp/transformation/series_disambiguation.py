import pandas as pd
from typing import List

def filter_important_dimensions(
    df: pd.DataFrame,
    base_columns: List[str],
    value_column: str,
    important_columns: List[str]
) -> (pd.DataFrame, List[str]):
    """
    Filters columns from the DataFrame by retaining only those dimensions that:
    - Either introduce new unique values to the dataset when grouped by the base columns.
    - Or are specified as important columns based on domain knowledge.

    Parameters:
    -----------
    df : pd.DataFrame
        The input DataFrame containing time series data in long format.
    base_columns : List[str]
        The core columns that represent the primary identifiers (e.g., 'country', 'year').
    value_column : str
        The column that contains the target values (e.g., 'value').
    important_columns : List[str]
        Columns that should be retained manually, even if they don't introduce new values (e.g., 'sex', 'location').

    Returns:
    --------
    filtered_df : pd.DataFrame
        The filtered DataFrame containing only the base columns, important dimensions, and the value column.
    columns_to_keep : List[str]
        The list of columns retained after filtering.

    Example Usage:
    --------------
    base_columns = ['country', 'year']
    value_column = 'value'
    important_columns = ['sex', 'location']

    filtered_df, retained_columns = filter_important_dimensions(
        df, base_columns, value_column, important_columns
    )
    print("Retained columns:", retained_columns)
    """
    dimensions = [col for col in df.columns if col not in base_columns + [value_column]]
    columns_to_keep = base_columns.copy()
    for dim in dimensions:
        if dim in important_columns:
            columns_to_keep.append(dim)
            continue

        grouped_with_dim = df.groupby(base_columns + [dim])[value_column].nunique()
        grouped_without_dim = df.groupby(base_columns)[value_column].nunique()

        if grouped_with_dim > grouped_without_dim:
            columns_to_keep.append(dim)

    filtered_df = df[columns_to_keep + [value_column]]
    return filtered_df, columns_to_keep
