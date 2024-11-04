from typing import Any

import numpy as np
import pandas as pd
from scipy.interpolate import CubicSpline, interp1d

__all__ = ["interpolate_data", "rename_indicator", "invert_dictionary"]


async def interpolate_data(
    data_frame: pd.DataFrame, target_column: Any = None, min_year: int = None
):
    """
    Interpolates missing data in a DataFrame's column using cubic spline interpolation.

    Args:
        data_frame (pd.DataFrame): The input DataFrame containing the data to interpolate.
        target_column (str): The column name for which interpolation should be performed.
        min_year (int, optional): The minimum year to consider for interpolation. If not provided,
                                  it defaults to 5 years before the earliest year in the data or 2000.

    Returns:
        tuple: A tuple containing three elements -
               1. A NumPy array of interpolated years.
               2. A NumPy array of interpolated values corresponding to the target column.
               3. The cleaned DataFrame after interpolation.
    """
    # Drop rows with missing values
    cleaned_df = data_frame.dropna()

    # If there are too few data points, return the cleaned data as is
    if cleaned_df.shape[0] < 2:
        return (
            cleaned_df["year"].tolist(),
            cleaned_df[target_column].tolist(),
            cleaned_df,
        )

    # Determine the range of years for interpolation
    max_y = cleaned_df["year"].max()
    min_y = cleaned_df["year"].min()
    years_max = min(int(max_y) + 6, CURRENT_YEAR)  # Adding a buffer of 6 years

    # Set the minimum year for interpolation
    if min_year:
        year_min = float(min_year)
    else:
        year_min = max(int(min_y) - 5, 2000)

    # Handle the case where max_year is provided
    if int(max_y) == CURRENT_YEAR:
        max_y = CURRENT_YEAR - 1
        cleaned_df = cleaned_df[cleaned_df["year"] != CURRENT_YEAR]
        if cleaned_df.shape[0] < 2:
            return (
                cleaned_df["year"].tolist(),
                cleaned_df[target_column].tolist(),
                cleaned_df,
            )

    # Generate arrays for interpolation
    interpolated_years = np.arange(year_min, years_max)
    within_range_interpolated_years = np.arange(int(min_y), int(max_y) + 1)
    # Perform cubic spline interpolation
    cubic_spline = CubicSpline(
        cleaned_df["year"], cleaned_df[target_column], bc_type="natural"
    )
    cubic_interpolated_values = cubic_spline(within_range_interpolated_years)

    # Perform linear interpolation for the entire range
    linear_interpolator = interp1d(
        within_range_interpolated_years,
        cubic_interpolated_values,
        fill_value="extrapolate",
    )
    linear_interpolated_values = linear_interpolator(interpolated_years)

    return interpolated_years, linear_interpolated_values, cleaned_df


async def rename_indicator(indicator: str, year: str | float):
    """
    Renames an indicator by appending the year to its name.

    Args:
        indicator (str): The name of the indicator.
        year (float or str): The year to append.

    Returns:
        str: The renamed indicator.
    """
    return indicator + "_" + str(int(float(year)))


async def invert_dictionary(original_dictionary):
    """
    Inverts the key-value pairs of a dictionary.

    Args:
        original_dictionary (dict): The original dictionary to invert.

    Returns:
        dict: The inverted dictionary.
    """
    inverted_dictionary = {}
    for dict_key in original_dictionary:
        # Swap the key and value in the inverted dictionary
        inverted_dictionary[original_dictionary[dict_key]] = dict_key
    return inverted_dictionary
