import asyncio
import io
import itertools
import json
import logging
import warnings
import os
import datetime

import numpy as np
import pandas as pd
from scipy.interpolate import CubicSpline, interp1d
from pathlib import Path
from dfpp.dfpp_exceptions import TransformationError, TransformationWarning
from dfpp.storage import StorageManager
from dfpp.constants import COUNTRY_LOOKUP_CSV_PATH, STANDARD_KEY_COLUMN, CURRENT_YEAR
from typing import List, Union

logger = logging.getLogger(__name__)




async def add_country_code(source_df, country_name_column=None):
    """
    Adds country codes to a DataFrame based on a country lookup table.

    Args:
        source_df (pandas.DataFrame): The source DataFrame to add country codes to.

    Returns:
        pandas.DataFrame: The source DataFrame with country codes added.
        :param source_df:
        :param country_key_column:
        :param country_name_column:

    """
    pd.set_option('display.max_rows', None)
    async with StorageManager() as storage_manager:
        country_lookup_bytes = await storage_manager.cached_download(source_path=COUNTRY_LOOKUP_CSV_PATH)
        country_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_lookup")
        if country_name_column != country_df.index.name:
            country_df.set_index(country_name_column, inplace=True)
            country_df.dropna(subset=[STANDARD_KEY_COLUMN], inplace=True)
        if country_name_column != source_df.index.name:
            source_df.set_index(country_name_column, inplace=True)
        source_df = source_df.join(country_df[STANDARD_KEY_COLUMN])
        source_df.reset_index(inplace=True)
        return source_df


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
    try:
        async with StorageManager() as storage_manager:
            country_lookup_bytes = await storage_manager.cached_download(source_path=COUNTRY_LOOKUP_CSV_PATH)
            region_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="region_lookup")
            region_df = region_df.drop_duplicates(subset=["Region"], keep='last')
            region_df.dropna(subset=["Region"], inplace=True)
            if "Region" != region_df.index.name:
                region_df['Region'] = region_df['Region'].str.replace(' ', '')
                region_df = region_df.drop_duplicates(subset=["Region"], keep='last')
                region_df.set_index("Region", inplace=True)
                region_df.dropna(subset=[STANDARD_KEY_COLUMN], inplace=True)

            if region_name_col != source_df.index.name:
                source_df[region_name_col] = source_df[region_name_col].str.replace(' ', '')
                source_df.set_index(region_name_col, inplace=True)
            if region_key_col is None:
                col_list = source_df.columns.to_list()
                if STANDARD_KEY_COLUMN in col_list:
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
    except Exception as e:
        logger.error("Error in add_region_code: {}".format(e))
        raise


async def add_alpha_code_3_column(source_df, country_col):
    """
    Adds the Alpha-3 country code column to a DataFrame based on a country lookup table.

    Args:
        source_df (pandas.DataFrame): The source DataFrame to add the Alpha-3 country code column to.
        country_col (str): The name of the column in `source_df` containing country names.

    Returns:
        pandas.DataFrame: The source DataFrame with the Alpha-3 country code column added.

    Raises:
        ValueError: If the country column is not present in the source DataFrame or the country lookup table.

        """
    async with StorageManager() as storage_manager:
        country_lookup_bytes = await storage_manager.cached_download(source_path=COUNTRY_LOOKUP_CSV_PATH)
        country_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_lookup")
        country_df = country_df.rename(columns={'Country': country_col})
        return pd.merge(source_df, country_df[['Alpha-3 code', country_col]], on=[country_col], how='left')


async def fix_iso_country_codes(df: pd.DataFrame = None, col: str = None, source_id=None):
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
    async with StorageManager() as storage_manager:
        country_lookup_bytes = await storage_manager.cached_download(source_path=COUNTRY_LOOKUP_CSV_PATH)
        country_code_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_code_lookup")
        for index, row in country_code_df.iterrows():
            if source_id is None or source_id == row.get("Only For Source ID"):
                df.loc[(df[col] == row['ISO 3 Code']), col] = row['UNDP ISO 3 Code']


async def change_iso3_to_system_region_iso3(source_df, iso3_col):
    """
    Changes ISO-3 country codes in a DataFrame to system region ISO-3 codes.

    Args:
        source_df (pandas.DataFrame): The source DataFrame containing ISO-3 country codes.
        iso3_col (str): The name of the column in `source_df` containing ISO-3 country codes.

    Returns:
        pandas.DataFrame: The modified DataFrame with ISO-3 country codes changed to system region ISO-3 codes.
        """
    temp_col = iso3_col + '_RegionDim'
    source_df[temp_col] = source_df[iso3_col]
    source_df = await add_region_code(source_df, temp_col, iso3_col)
    source_df[iso3_col] = source_df[STANDARD_KEY_COLUMN]
    drop_list = [temp_col]
    if iso3_col != STANDARD_KEY_COLUMN:
        drop_list.append(STANDARD_KEY_COLUMN)
    return source_df.drop(columns=drop_list)


async def get_year_columns(columns, col_prefix=None, col_suffix=None, column_substring=None, indicator_id=None):
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
        except Exception as e:
            pass
    # if not year_columns:
    #     raise RuntimeError(f'Could not establish year data columns for {indicator_id}')
    return year_columns


async def rename_indicator(indicator, year):
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


async def country_group_dataframe():
    """
    Retrieves the country and territory group DataFrame from Azure Blob Storage.

    Returns:
        pandas.DataFrame: The country and territory group DataFrame.
    """
    async with StorageManager() as storage_manager:
        try:
            # Read the country and territory group DataFrame from JSON in Azure Blob Storage
            df = pd.read_json(io.BytesIO(await storage_manager.cached_download(
                source_path=os.path.join(
                    storage_manager.ROOT_FOLDER,
                    'config',
                    'utilities',
                    'country_territory_groups.json'
                )
            )))

            # Rename the 'Alpha-3 code-1' column to 'Alpha-3 code'
            df.rename(columns={"Alpha-3 code-1": "Alpha-3 code"}, inplace=True)

            # Replace empty strings with NaN values
            df['Longitude (average)'] = df['Longitude (average)'].replace(r'', np.NaN)

            # Convert 'Latitude (average)' and 'Longitude (average)' columns to float64
            df['Latitude (average)'] = df["Latitude (average)"].astype(np.float64)
            df['Longitude (average)'] = df["Longitude (average)"].astype(np.float64)

            # TODO: The following code is commented out because the aggregate territory group DataFrame is no longer needed.
            # Read the aggregate territory group DataFrame from JSON in Azure Blob Storage
            # df_aggregates = pd.read_json(io.BytesIO(await storage_manager.cached_download(
            #     source_path=os.path.join(
            #         storage_manager.ROOT_FOLDER,
            #         'config',
            #         'utilities',
            #         'aggregate_territory_groups.json'
            #     )
            # )))
            #
            # # Rename the 'Alpha-3 code-1' column to 'Alpha-3 code'
            # df_aggregates.rename(columns={"Alpha-3 code-1": "Alpha-3 code"}, inplace=True)
            #
            # # Replace empty strings with NaN values
            # df_aggregates['Longitude (average)'] = df_aggregates['Longitude (average)'].replace(r'', np.NaN)
            #
            # # Convert 'Latitude (average)' and 'Longitude (average)' columns to float64
            # df_aggregates['Latitude (average)'] = df_aggregates["Latitude (average)"].astype(np.float64)
            # df_aggregates['Longitude (average)'] = df_aggregates["Longitude (average)"].astype(np.float64)
            #
            # # Concatenate the country group and aggregate territory group DataFrames
            # df = pd.concat([df, df_aggregates], ignore_index=True)

            return df
        except Exception as e:
            logger.error("Error retrieving country group DataFrame: " + str(e))
            return pd.DataFrame()  # Return an empty DataFrame if an error occurs


async def region_group_dataframe():
    """
    Retrieves the region group DataFrame from Azure Blob Storage.

    Returns:
        pandas.DataFrame: The region group DataFrame.
    """
    async with StorageManager() as storage_manager:
        try:
            # Read the region group DataFrame from JSON in Azure Blob Storage
            df = pd.read_json(io.BytesIO(await storage_manager.cached_download(
                source_path=os.path.join(
                    storage_manager.ROOT_FOLDER,
                    'config',
                    'utilities',
                    'aggregate_territory_groups.json'
                )
            )))

            # Rename the 'Alpha-3 code-1' column to 'Alpha-3 code'
            df.rename(columns={"Alpha-3 code-1": "Alpha-3 code"}, inplace=True)

            # Replace empty strings with NaN values
            df['Longitude (average)'] = df['Longitude (average)'].replace(r'', np.NaN)

            # Convert 'Latitude (average)' and 'Longitude (average)' columns to float64
            df['Latitude (average)'] = df["Latitude (average)"].astype(np.float64)
            df['Longitude (average)'] = df["Longitude (average)"].astype(np.float64)

            return df
        except Exception as e:
            logger.error("Error retrieving region group DataFrame: " + str(e))
            return pd.DataFrame()  # Return an empty DataFrame if an error occurs


def chunker(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


async def validate_indicator_transformed(storage_manager: StorageManager, indicator_id: str = None,
                                         pre_update_checksum: str = None,
                                         df: pd.DataFrame = None):
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
    assert indicator_id is not None, "Indicator ID is required"
    assert df is not None, "DataFrame is required"
    assert isinstance(df, pd.DataFrame), "DataFrame must be a pandas DataFrame"
    assert pre_update_checksum is not None, "Base DataFrame checksum is required"
    try:
        indicator_configuration = await storage_manager.get_indicator_cfg(indicator_id=indicator_id)
        source_id = indicator_configuration.get("indicator").get("source_id")
        base_file_name = f"{source_id}.csv"

        df_columns = df.columns.to_list()
        columns_with_indicators = [column for column in df_columns if column.startswith(f"{indicator_id}_")]

        # Check that the indicator columns are present
        if len(columns_with_indicators) == 0:
            raise TransformationError(
                f"Indicator {indicator_id} not found in columns of base file {source_id}.csv. This is likely due to a transformation error that occurred during the transformation process. Please check the transformation logs for more details.")
        years_columns = await get_year_columns(columns=df_columns, col_prefix=f"{indicator_id}_")

        # if the columns are present, check that all the columns have at least some data
        indicator_base_columns = [column for column in df_columns if column in years_columns.values()]
        base_with_data = df[indicator_base_columns].dropna(how='all')
        if base_with_data.empty:
            raise TransformationError(
                f"Indicator {indicator_id} has no data. This is likely due to a transformation error that occurred during the transformation process or absent data in the source file. Please check the transformation logs for more details.")

        # Compare previous md5 checksum with current md5 checksum
        md5_checksum = await storage_manager.get_md5_checksum(
            blob_name=os.path.join(storage_manager.ROOT_FOLDER, 'output', 'access_all_data', 'base', base_file_name))
        if md5_checksum == pre_update_checksum:
            warnings.warn(
                f"Base file {base_file_name} has not changed since the last transformation. This could be because of no new data since previous transformation, or that transformation failed for indicator {indicator_id}.",
                TransformationWarning)
    except Exception as e:
        logger.error(f"Error validating indicator {indicator_id}: {e}")
        raise


async def update_base_file(indicator_id: str = None, df: pd.DataFrame = None, blob_name: str = None,
                           project: str = None):
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

    async with StorageManager() as storage_manager:
        pre_update_md5_checksum = await storage_manager.get_md5_checksum(
            blob_name=os.path.join(storage_manager.ROOT_FOLDER, 'output', project, 'base', blob_name),
            data=df.to_csv().encode('utf-8')
        )
        try:
            # Reset the index of the DataFrame
            df.reset_index(inplace=True)

            # Drop rows with missing values in the STANDARD_KEY_COLUMN
            df.dropna(subset=[STANDARD_KEY_COLUMN], inplace=True)

            # Check if the blob file already exists in Azure Blob Storage
            blob_exists = await storage_manager.check_blob_exists(
                blob_name=os.path.join(storage_manager.ROOT_FOLDER, 'output', project, 'base', blob_name)
            )

            # Create an empty DataFrame for the keys
            key_df = pd.DataFrame(columns=[STANDARD_KEY_COLUMN])

            # Get the country group DataFrame
            country_group_df = await country_group_dataframe()

            if blob_exists:
                logger.info(f"Base file {blob_name} exists. Updating...")
                # Download the base file as bytes and read it as a DataFrame
                base_file_bytes = await storage_manager.cached_download(
                    source_path=os.path.join(storage_manager.ROOT_FOLDER, 'output', project, 'base', blob_name),
                )
                base_file_df = pd.read_csv(io.BytesIO(base_file_bytes))
                # base_file_df = pd.DataFrame(columns=[STANDARD_KEY_COLUMN])
            else:
                logger.info("Base file does not exist. Creating...")
                base_file_df = pd.DataFrame(columns=[STANDARD_KEY_COLUMN])
                base_file_df[STANDARD_KEY_COLUMN] = country_group_df[STANDARD_KEY_COLUMN]

            # Filter base_file_df to include only rows with keys present in country_group_df
            base_file_df = base_file_df[base_file_df[STANDARD_KEY_COLUMN].isin(country_group_df[STANDARD_KEY_COLUMN])]
            # Select the keys from country_group_df that are not present in base_file_df
            key_df[STANDARD_KEY_COLUMN] = \
                country_group_df[~country_group_df[STANDARD_KEY_COLUMN].isin(base_file_df[STANDARD_KEY_COLUMN])][
                    STANDARD_KEY_COLUMN]

            # Concatenate base_file_df and key_df
            base_file_df = pd.concat([base_file_df, key_df], ignore_index=True)

            # Set STANDARD_KEY_COLUMN as the index for base_file_df and df
            base_file_df.set_index(STANDARD_KEY_COLUMN, inplace=True)
            df.set_index(STANDARD_KEY_COLUMN, inplace=True)
            # Update columns that exist in both base_file_df and df
            update_cols = list(set(base_file_df.columns.to_list()).intersection(set(df.columns.to_list())))
            base_file_df.update(df[update_cols])

            # Join columns from df that are not present in base_file_df
            add_columns = list(set(df.columns.to_list()) - set(base_file_df.columns.to_list()))
            base_file_df = base_file_df.join(df[add_columns])

            # Reset the index of base_file_df
            # this will add a new int column with new indices
            # base_file_df.reset_index(inplace=True)

            # Define the destination path for the CSV file
            destination_path = os.path.join(storage_manager.ROOT_FOLDER, 'output', project, 'base', blob_name)

            await storage_manager.upload(
                data=base_file_df.to_csv(encoding='utf-8'),
                dst_path=destination_path,
                overwrite=True,
                content_type="text/csv"
            )
            await validate_indicator_transformed(storage_manager=storage_manager, indicator_id=indicator_id,
                                                 df=base_file_df, pre_update_checksum=pre_update_md5_checksum)
        except Exception as e:
            logger.error(f"Error uploading to blob: {e}")
            raise


async def list_command(
        indicators=False,
        sources=False,
        config=True,

) -> List[str]:
    async with StorageManager() as storage_manager:
        logger.debug(f'Connected to Azure blob')
        if sources:
            source_files = await storage_manager.list_sources_cfgs()
            source_ids = [os.path.split(sf)[-1].split('.cfg')[0].upper() for sf in source_files]

        if indicators:
            indicator_files = [indicator_blob.name async for indicator_blob in storage_manager.list_indicators()]
            indicator_ids = [os.path.split(sf)[-1].split('.cfg')[0] for sf in indicator_files]
        '''
            the code below could be sued if we decide to validate the configs and list only those that are valid
        '''
        # if sources:
        #     source_configs = await storage_manager.get_sources_cfgs()
        #     source_ids = [scfg['source']['id'] for scfg in source_configs]
        #
        # if indicators:
        #     indicator_configs = await storage_manager.get_indicators_cfg()
        #     indicator_ids = [icfg['indicator']['indicator_id'] for icfg in indicator_configs]
        if config:
            keys = ['ROOT_FOLDER', 'INDICATORS_CFG_PATH', 'SOURCES_CFG_PATH', 'UTILITIES_PATH', 'SOURCES_PATH',
                    'OUTPUT_PATH', 'container_name', 'conn_str']
            pipeline_cfg = dict()
            for k in keys:
                pipeline_cfg[k] = getattr(storage_manager, k, None)

        if indicators:
            logger.info(f'{len(indicator_ids)} indicators were detected: {json.dumps(indicator_ids, indent=4)}')
        if sources:
            logger.info(f'{len(source_ids)} indicator sources were detected: {json.dumps(source_ids, indent=4)}')
        if config:
            logger.info(f'Pipeline configuration: {json.dumps([pipeline_cfg], indent=4)}')


async def interpolate_data(data_frame: pd.DataFrame, target_column=None, min_year=None):
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
        return cleaned_df['year'].tolist(), cleaned_df[target_column].tolist(), cleaned_df

    # Determine the range of years for interpolation
    max_y = cleaned_df['year'].max()
    min_y = cleaned_df['year'].min()
    years_max = min(int(max_y) + 6, CURRENT_YEAR)  # Adding a buffer of 6 years

    # Set the minimum year for interpolation
    if min_year:
        year_min = float(min_year)
    else:
        year_min = max(int(min_y) - 5, 2000)

    # Handle the case where max_year is provided
    if int(max_y) == CURRENT_YEAR:
        max_y = CURRENT_YEAR - 1
        cleaned_df = cleaned_df[cleaned_df['year'] != CURRENT_YEAR]
        if cleaned_df.shape[0] < 2:
            return cleaned_df['year'].tolist(), cleaned_df[target_column].tolist(), cleaned_df

    # Generate arrays for interpolation
    interpolated_years = np.arange(year_min, years_max)
    within_range_interpolated_years = np.arange(int(min_y), int(max_y) + 1)
    # Perform cubic spline interpolation
    cubic_spline = CubicSpline(cleaned_df['year'], cleaned_df[target_column], bc_type='natural')
    cubic_interpolated_values = cubic_spline(within_range_interpolated_years)

    # Perform linear interpolation for the entire range
    linear_interpolator = interp1d(within_range_interpolated_years, cubic_interpolated_values, fill_value='extrapolate')
    linear_interpolated_values = linear_interpolator(interpolated_years)

    return interpolated_years, linear_interpolated_values, cleaned_df

if __name__ == "__main__":
    test_df = pd.read_csv("./BTI_PROJECT.csv")
    asyncio.run(update_base_file(df=test_df, blob_name="BTI_PROJECT.csv"))
    # asyncio.run(country_group_dataframe())
