import asyncio
import io
import itertools
import json
import logging
import os
import warnings
from typing import Any

import numpy as np
import pandas as pd
from scipy.interpolate import CubicSpline, interp1d

from .constants import CURRENT_YEAR, STANDARD_KEY_COLUMN
from .exceptions import TransformationError, TransformationWarning
from .storage import StorageManager
from dfpp.exceptions_logger import handle_exceptions

logger = logging.getLogger(__name__)

@StorageManager.with_storage_manager
async def _get_country_df(country_name_column=None, storage_manager=None):
    country_df = await storage_manager.get_lookup_df(sheet="country")
    if country_name_column != country_df.index.name:
        country_df.set_index(country_name_column, inplace=True)
        country_df.dropna(subset=[STANDARD_KEY_COLUMN], inplace=True)
    return country_df


@handle_exceptions(logger=logger)
async def add_country_code(source_df, country_name_column=None):
    """
    Adds country codes to a DataFrame based on a country lookup table.

    Args:
        source_df (pandas.DataFrame): The source DataFrame to add country codes to.
        country_name_column (str, optional): The name of the column in `source_df`
                                             containing country names.

    Returns:
        pandas.DataFrame: The source DataFrame with country codes added.
    """

    country_df = await _get_country_df(country_name_column)

    if country_name_column != source_df.index.name:
        source_df.set_index(country_name_column, inplace=True)

    source_df = source_df.join(country_df[STANDARD_KEY_COLUMN])
    source_df.reset_index(inplace=True)
    
    return source_df

@StorageManager.with_storage_manager
async def _get_region_df(storage_manager=None):
    region_df = await storage_manager.get_lookup_df(sheet="region")
    region_df = region_df.drop_duplicates(subset=["Region"], keep="last")
    region_df.dropna(subset=["Region"], inplace=True)
    if "Region" != region_df.index.name:
        region_df["Region"] = region_df["Region"].str.replace(" ", "")
        region_df = region_df.drop_duplicates(subset=["Region"], keep="last")
        region_df.set_index("Region", inplace=True)
        region_df.dropna(subset=[STANDARD_KEY_COLUMN], inplace=True)
    return region_df

@handle_exceptions(logger=logger)
async def add_region_code(source_df=None, region_name_col=None, region_key_col=None):
    """
    Adds region codes to a DataFrame based on a lookup table.

    Args:
        source_df (pandas.DataFrame): The source DataFrame to add region codes to.
        region_name_col (str): The name of the column in `source_df` containing region names.
        region_key_col (str, optional): The name of the column in the region lookup table
            containing region codes. If not provided, the function will attempt to match
            the column name with the region lookup table automatically. Defaults to None.

    Returns:
        pandas.DataFrame: The source DataFrame with region codes added.

    Raises:
        ValueError: If the region name column is not present in the source DataFrame
            or the region lookup table.
    """

    region_df = await _get_region_df()

    if region_name_col != source_df.index.name:
        source_df[region_name_col] = source_df[region_name_col].str.replace(" ", "")
        source_df.set_index(region_name_col, inplace=True)

    if region_key_col is None:
        if STANDARD_KEY_COLUMN in source_df.columns:
            source_df.update(region_df[STANDARD_KEY_COLUMN])
        else:
            source_df = source_df.join(region_df[STANDARD_KEY_COLUMN])
    else:
        region_df.rename(columns={STANDARD_KEY_COLUMN: region_key_col}, inplace=True)
        source_df.update(region_df[region_key_col])
        source_df.rename(columns={region_key_col: STANDARD_KEY_COLUMN}, inplace=True)

    source_df.reset_index(inplace=True)
    source_df.dropna(subset=[STANDARD_KEY_COLUMN], inplace=True)
    
    return source_df


async def add_alpha_code_3_column(source_df, country_name_column):
    """
    Adds the Alpha-3 country code column to a DataFrame based on a country lookup table.

    Args:
        source_df (pandas.DataFrame): The source DataFrame to add the Alpha-3 country code column to.
        country_name_column (str): The name of the column in `source_df` containing country names.

    Returns:
        pandas.DataFrame: The source DataFrame with the Alpha-3 country code column added.

    Raises:
        ValueError: If the country column is not present in the source DataFrame or the country lookup table.

    """
    country_df = await _get_country_df(country_name_column)
    return pd.merge(
            source_df,
            country_df[["Alpha-3 code", country_name_column]],
            on=[country_name_column],
            how="left",
        )

@StorageManager.with_storage_manager
async def _get_country_code_df(storage_manager=None):
    country_code_df = await storage_manager.get_lookup_df(sheet="country_code")
    return country_code_df


async def fix_iso_country_codes(
    df: pd.DataFrame = None, col: str = None, source_id=None
):
    """
    Fixes ISO country codes in a DataFrame based on a country code lookup table.

    Args:
        df (pandas.DataFrame): The DataFrame to fix the ISO country codes in.
        col (str): The name of the column containing ISO country codes in `df`.
        source_id (str, optional): The source ID for which to fix the ISO country codes.
            If provided, only rows with a matching source ID will be updated. Defaults to None.

    Returns:
        None: The function modifies the provided DataFrame in place.

    """

    country_code_df = await _get_country_code_df()
    for _, row in country_code_df.iterrows():
        if source_id is None or source_id == row.get("Only For Source ID"):
            df.loc[(df[col] == row["ISO 3 Code"]), col] = row["UNDP ISO 3 Code"]


async def change_iso3_to_system_region_iso3(source_df, iso3_col):
    """
    Changes ISO-3 country codes in a DataFrame to system region ISO-3 codes.

    Args:
        source_df (pandas.DataFrame): The source DataFrame containing ISO-3 country codes.
        iso3_col (str): The name of the column in `source_df` containing ISO-3 country codes.

    Returns:
        pandas.DataFrame: The modified DataFrame with ISO-3 country codes changed to system region ISO-3 codes.
    """
    temp_col = iso3_col + "_RegionDim"
    source_df[temp_col] = source_df[iso3_col]
    source_df = await add_region_code(source_df, temp_col, iso3_col)
    source_df[iso3_col] = source_df[STANDARD_KEY_COLUMN]
    drop_list = [temp_col]
    if iso3_col != STANDARD_KEY_COLUMN:
        drop_list.append(STANDARD_KEY_COLUMN)
    return source_df.drop(columns=drop_list)


async def get_year_columns(
    columns, col_prefix=None, col_suffix=None, column_substring=None, indicator_id=None
):
    """
    Extracts year-based columns from a list of column names.

    Args:
        columns (list): The list of column names.
        col_prefix (str, optional): Prefix to remove from column names. Defaults to None.
        col_suffix (str, optional): Suffix to remove from column names. Defaults to None.
        column_substring (str, optional): Substring to remove from column names. Defaults to None.

    Returns:
        dict: A dictionary mapping year values to corresponding column names.
    """
    year_columns = {}  # Dictionary to store year-based columns
    column_map = {}  # Dictionary to map original column names to modified column names

    # Map each column to itself initially
    for column in columns:
        column_map[column] = column
    # Remove column substring if specified
    if column_substring is not None:
        for column in columns:
            column_map[column] = str(column_map[column]).replace(column_substring, "")
    # Remove col_prefix and/or col_suffix if specified
    elif col_prefix is not None or col_suffix is not None:
        if col_prefix is not None:
            for column in columns:
                column_map[column] = column_map[column].rsplit(col_prefix)[-1]
        if col_suffix is not None:
            for column in columns:
                column_map[column] = column_map[column].rsplit(col_suffix)[0]

    # Extract year values and map them to corresponding column names
    for column in column_map:
        try:
            year = int(float(column_map[column]))
            year_columns[year] = column
        except:
            pass
    # if not year_columns:
    #     raise RuntimeError(f'Could not establish year data columns for {indicator_id}')
    return year_columns


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


@handle_exceptions(logger=logger)
@StorageManager.with_storage_manager
async def country_group_dataframe(storage_manager=None):
    """
    Retrieves the country and territory group DataFrame from Azure Blob Storage.

    Returns:
        pandas.DataFrame: The country and territory group DataFrame.
    """
    # Read the country and territory group DataFrame from JSON in Azure Blob Storage
    path = os.path.join("config", "utilities", "country_territory_groups.json")
    data = await storage_manager.read_blob(path=path)
    df = pd.read_json(io.BytesIO(data))

    # Rename the 'Alpha-3 code-1' column to 'Alpha-3 code'
    df.rename(columns={"Alpha-3 code-1": "Alpha-3 code"}, inplace=True)

    # Replace empty strings with NaN values
    df["Longitude (average)"] = df["Longitude (average)"].replace(r"", np.NaN)

    # Convert 'Latitude (average)' and 'Longitude (average)' columns to float64
    df["Latitude (average)"] = df["Latitude (average)"].astype(np.float64)
    df["Longitude (average)"] = df["Longitude (average)"].astype(np.float64)

    # TODO: Add the regions to the country group DataFrame
    # Read the aggregate territory group DataFrame from JSON in Azure Blob Storage
    path = os.path.join(
        "config", "utilities", "aggregate_territory_groups.json"
    )
    data = await storage_manager.read_blob(path=path)
    df_aggregates = pd.read_json(io.BytesIO(data))

    # Rename the 'Alpha-3 code-1' column to 'Alpha-3 code'
    df_aggregates.rename(
        columns={"Alpha-3 code-1": "Alpha-3 code"}, inplace=True
    )
    #
    # Replace empty strings with NaN values
    df_aggregates["Longitude (average)"] = df_aggregates[
        "Longitude (average)"
    ].replace(r"", np.NaN)
    #
    # Convert 'Latitude (average)' and 'Longitude (average)' columns to float64
    df_aggregates["Latitude (average)"] = df_aggregates[
        "Latitude (average)"
    ].astype(np.float64)
    df_aggregates["Longitude (average)"] = df_aggregates[
        "Longitude (average)"
    ].astype(np.float64)

    # Concatenate the country group and aggregate territory group DataFrames
    df = pd.concat([df, df_aggregates], ignore_index=True)

    return df

@handle_exceptions(logger=logger)
@StorageManager.with_storage_manager
async def region_group_dataframe(storage_manager=None):
    """
    Retrieves the region group DataFrame from Azure Blob Storage.

    Returns:
        pandas.DataFrame: The region group DataFrame.
    """
    # Read the region group DataFrame from JSON in Azure Blob Storage
    path = os.path.join(
        "config", "utilities", "aggregate_territory_groups.json"
    )
    data = await storage_manager.read_blob(path=path)
    df = pd.read_json(io.BytesIO(data))

    # Rename the 'Alpha-3 code-1' column to 'Alpha-3 code'
    df.rename(columns={"Alpha-3 code-1": "Alpha-3 code"}, inplace=True)

    # Replace empty strings with NaN values
    df["Longitude (average)"] = df["Longitude (average)"].replace(r"", np.NaN)

    # Convert 'Latitude (average)' and 'Longitude (average)' columns to float64
    df["Latitude (average)"] = df["Latitude (average)"].astype(np.float64)
    df["Longitude (average)"] = df["Longitude (average)"].astype(np.float64)

    return df


def chunker(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk

@handle_exceptions(logger=logger)
@StorageManager.with_storage_manager
async def validate_indicator_transformed(
    
    indicator_id: str = None,
    pre_update_checksum: str | None = None,
    df: pd.DataFrame = None,
    storage_manager: StorageManager = None
):
    """
    Validates that a transformed indicator DataFrame contains the required columns.

    Args:
        storage_manager (StorageManager): The StorageManager instance.
        indicator_id (str): The indicator ID.
        pre_update_checksum (str): The checksum of the base DataFrame.
        df (pandas.DataFrame): The transformed indicator DataFrame.

    Raises:
        ValueError: If the DataFrame does not contain the required columns.

    """
    assert df is not None, "DataFrame is required"
    assert isinstance(df, pd.DataFrame), "DataFrame must be a pandas DataFrame"

    indicator_configuration = await storage_manager.get_indicator_cfg(
        indicator_id=indicator_id
    )
    source_id = indicator_configuration.get("indicator").get("source_id")
    base_file_name = f"{source_id}.csv"

    df_columns = df.columns.to_list()
    columns_with_indicators = [
        column for column in df_columns if column.startswith(f"{indicator_id}_")
    ]

    # Check that the indicator columns are present
    if len(columns_with_indicators) == 0:
        raise TransformationError(
            f"Indicator {indicator_id} not found in columns of base file {source_id}.csv. This is likely due to a transformation error that occurred during the transformation process. Please check the transformation logs for more details."
        )
    years_columns = await get_year_columns(
        columns=df_columns, col_prefix=f"{indicator_id}_"
    )

    # if the columns are present, check that all the columns have at least some data
    indicator_base_columns = [
        column for column in df_columns if column in years_columns.values()
    ]
    base_with_data = df[indicator_base_columns].dropna(how="all")
    if base_with_data.empty:
        raise TransformationError(
            f"Indicator {indicator_id} has no data. This is likely due to a transformation error that occurred during the transformation process or absent data in the source file. Please check the transformation logs for more details."
        )

    # Compare previous md5 checksum with current md5 checksum
    path = os.path.join("output", "access_all_data", "base", base_file_name)
    md5_checksum = await storage_manager.get_md5(path=path)
    if pre_update_checksum is None:
        warnings.warn(
            f"pre_update_checksum is None, indicating that the base file did not exist and was created after the transformation for indicator {indicator_id}.",
            TransformationWarning,
        )
    elif md5_checksum == pre_update_checksum:
        warnings.warn(
            f"Base file {base_file_name} has not changed since the last transformation. This could be because of no new data since previous transformation, or that transformation failed for indicator {indicator_id}.",
            TransformationWarning,
        )

@handle_exceptions(logger=logger)
@StorageManager.with_storage_manager
async def update_base_file(
    indicator_id: str = None,
    df: pd.DataFrame = None,
    blob_name: str = None,
    project: str = None,
    storage_manager: StorageManager = None
):
    """
    Uploads a DataFrame as a CSV file to Azure Blob Storage.

    Args:
        indicator_id (str): The indicator ID.
        df (pandas.DataFrame): The DataFrame to upload.
        blob_name (str): The name of the blob file in Azure Blob Storage.
        project (str): The project to upload the blob file to.
    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    path = os.path.join("output", project, "base", blob_name)
    pre_update_md5_checksum = await storage_manager.get_md5(path=path)

    # Reset the index of the DataFrame
    df.reset_index(inplace=True)

    # Drop rows with missing values in the STANDARD_KEY_COLUMN
    df.dropna(subset=[STANDARD_KEY_COLUMN], inplace=True)

    # Check if the blob file already exists in Azure Blob Storage
    blob_exists = await storage_manager.check_blob_exists(
        blob_name=os.path.join("output", project, "base", blob_name)
    )

    # Create an empty DataFrame for the keys
    key_df = pd.DataFrame(columns=[STANDARD_KEY_COLUMN])

    # Get the country group DataFrame
    country_group_df = await country_group_dataframe()

    if blob_exists:
        logger.info(f"Base file {blob_name} exists. Updating...")
        # Download the base file as bytes and read it as a DataFrame
        path = os.path.join("output", project, "base", blob_name)
        data = await storage_manager.read_blob(path=path)
        base_file_df = pd.read_csv(io.BytesIO(data))
        # base_file_df = pd.DataFrame(columns=[STANDARD_KEY_COLUMN])
    else:
        logger.info("Base file does not exist. Creating...")
        base_file_df = pd.DataFrame(columns=[STANDARD_KEY_COLUMN])
        base_file_df[STANDARD_KEY_COLUMN] = country_group_df[
            STANDARD_KEY_COLUMN
        ]

    # Filter base_file_df to include only rows with keys present in country_group_df
    base_file_df = base_file_df[
        base_file_df[STANDARD_KEY_COLUMN].isin(
            country_group_df[STANDARD_KEY_COLUMN]
        )
    ]
    # Select the keys from country_group_df that are not present in base_file_df
    key_df[STANDARD_KEY_COLUMN] = country_group_df[
        ~country_group_df[STANDARD_KEY_COLUMN].isin(
            base_file_df[STANDARD_KEY_COLUMN]
        )
    ][STANDARD_KEY_COLUMN]

    # Concatenate base_file_df and key_df
    base_file_df = pd.concat([base_file_df, key_df], ignore_index=True)

    # Set STANDARD_KEY_COLUMN as the index for base_file_df and df
    base_file_df.set_index(STANDARD_KEY_COLUMN, inplace=True)
    base_file_df.to_csv("base_file_df.csv")
    df.set_index(STANDARD_KEY_COLUMN, inplace=True)
    # Update columns that exist in both base_file_df and df
    update_cols = list(
        set(base_file_df.columns.to_list()).intersection(
            set(df.columns.to_list())
        )
    )

    base_file_df = base_file_df[~base_file_df.index.duplicated(keep="first")]
    duplicates = df.index[df.index.duplicated(keep=False)]
    if len(duplicates) > 0:
        logger.warning(
            f"Duplicate index found in df: {duplicates}....will keep the first row found"
        )
        # Drop rows with duplicate index
        df = df[~df.index.duplicated(keep="first")]
    base_file_df.update(df[update_cols])
    # Join columns from df that are not present in base_file_df
    add_columns = list(
        set(df.columns.to_list()) - set(base_file_df.columns.to_list())
    )
    base_file_df = base_file_df.join(df[add_columns])

    # Reset the index of base_file_df
    # this will add a new int column with new indices
    # base_file_df.reset_index(inplace=True)

    # Define the destination path for the CSV file
    path = os.path.join("output", project, "base", blob_name)
    await storage_manager.upload_blob(
        path_or_data_src=base_file_df.to_csv(encoding="utf-8"),
        path_dst=path,
        content_type="text/csv",
        overwrite=True,
    )

    await validate_indicator_transformed(
        storage_manager=storage_manager,
        indicator_id=indicator_id,
        df=base_file_df,
        pre_update_checksum=pre_update_md5_checksum,
    )


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

@StorageManager.with_storage_manager
async def base_df_for_indicator(
    indicator_id: str, project: str, storage_manager: StorageManager = None
) -> pd.DataFrame:
    """
    Read the base file for the indicator.

    This function retrieves the base file for a given indicator ID, reads it as a pandas DataFrame,
    and returns the DataFrame for further processing.

    :param storage_manager: The StorageManager object responsible for handling file operations.
    :param indicator_id: The ID of the indicator for which to retrieve the base file.
    :return: pd.DataFrame: The pandas DataFrame containing the data from the base file.
    """
    assert indicator_id is not None, "Indicator id is required"
    logger.info(f"Retrieving base file for indicator_id {indicator_id}")
    # Retrieve indicator configurations from the storage manager
    indicator_cfgs = await storage_manager.get_indicators_cfg(
        indicator_ids=[indicator_id]
    )

    cfg = indicator_cfgs[0]

    # Create the base file name based on the source_id from indicator configurations
    base_file_name = f"{cfg['indicator']['source_id']}.csv"

    # Create the base file path
    base_file_path = os.path.join(
        storage_manager.output_path, project, "base", base_file_name
    )

    logger.info(f"downloading base file {base_file_name}")
    # Read the base file as a pandas DataFrame
    data = await storage_manager.read_blob(path=base_file_path)
    base_file_df = pd.read_csv(io.BytesIO(data))

    return base_file_df

