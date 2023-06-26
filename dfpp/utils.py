import asyncio
import io
import logging
from tempfile import TemporaryDirectory
import os
import itertools
import numpy as np
import pandas as pd
from dfpp.storage import AsyncAzureBlobStorageManager
from dfpp.constants import COUNTRY_LOOKUP_CSV_PATH, STANDARD_KEY_COLUMN
AZURE_STORAGE_CONNECTION_STRING=os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
AZURE_STORAGE_CONTAINER_NAME=os.environ.get('AZURE_STORAGE_CONTAINER_NAME')
ROOT_FOLDER = os.environ.get('ROOT_FOLDER')

logger = logging.getLogger(__name__)

def chunker(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk

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
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=AZURE_STORAGE_CONNECTION_STRING,
        container_name=AZURE_STORAGE_CONTAINER_NAME,
        use_singleton=False
    )
    country_lookup_bytes = await storage.download(blob_name=COUNTRY_LOOKUP_CSV_PATH)
    country_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_lookup")
    if country_name_column != country_df.index.name:
        country_df.set_index(country_name_column, inplace=True)
        country_df.dropna(subset=[STANDARD_KEY_COLUMN], inplace=True)
    if country_name_column != source_df.index.name:
        source_df.set_index(country_name_column, inplace=True)
    source_df = source_df.join(country_df[STANDARD_KEY_COLUMN])
    source_df.reset_index(inplace=True)
    await storage.close()
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
        storage = await AsyncAzureBlobStorageManager.create_instance(
            connection_string=AZURE_STORAGE_CONNECTION_STRING,
            container_name=AZURE_STORAGE_CONTAINER_NAME,
            use_singleton=False
        )
        country_lookup_bytes = await storage.download(blob_name=COUNTRY_LOOKUP_CSV_PATH)
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
        await storage.close()
        return source_df
    except Exception as e:
        await storage.close()
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
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=AZURE_STORAGE_CONNECTION_STRING,
        container_name=AZURE_STORAGE_CONTAINER_NAME,
        use_singleton=False
    )
    country_lookup_bytes = await storage.download(COUNTRY_LOOKUP_CSV_PATH)
    country_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_lookup")
    country_df = country_df.rename(columns={'Country': country_col})
    await storage.close()
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
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=AZURE_STORAGE_CONNECTION_STRING,
        container_name=AZURE_STORAGE_CONTAINER_NAME,
        use_singleton=False
    )
    country_lookup_bytes = await storage.download(COUNTRY_LOOKUP_CSV_PATH)
    country_code_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_code_lookup")
    for index, row in country_code_df.iterrows():
        if source_id is None or source_id == row.get("Only For Source ID"):
            df.loc[(df[col] == row['ISO 3 Code']), col] = row['UNDP ISO 3 Code']
    await storage.close()


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


async def get_year_columns(columns, col_prefix=None, col_suffix=None, column_substring=None):
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
            column_map[column] = column_map[column].replace(column_substring, "")
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
    storage_manager = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=AZURE_STORAGE_CONNECTION_STRING,
        container_name=AZURE_STORAGE_CONTAINER_NAME,
        use_singleton=False
    )
    try:
        # Read the country and territory group DataFrame from JSON in Azure Blob Storage
        df = pd.read_json(io.BytesIO(await storage_manager.download(
            blob_name=os.path.join(
                ROOT_FOLDER,
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

        # Read the aggregate territory group DataFrame from JSON in Azure Blob Storage
        df_aggregates = pd.read_json(io.BytesIO(await storage_manager.download(
            blob_name=os.path.join(
                ROOT_FOLDER,
                'config',
                'utilities',
                'aggregate_territory_groups.json'
            )
        )))

        # Rename the 'Alpha-3 code-1' column to 'Alpha-3 code'
        df_aggregates.rename(columns={"Alpha-3 code-1": "Alpha-3 code"}, inplace=True)

        # Replace empty strings with NaN values
        df_aggregates['Longitude (average)'] = df_aggregates['Longitude (average)'].replace(r'', np.NaN)

        # Convert 'Latitude (average)' and 'Longitude (average)' columns to float64
        df_aggregates['Latitude (average)'] = df_aggregates["Latitude (average)"].astype(np.float64)
        df_aggregates['Longitude (average)'] = df_aggregates["Longitude (average)"].astype(np.float64)

        # Concatenate the country group and aggregate territory group DataFrames
        df = pd.concat([df, df_aggregates], ignore_index=True)

        await storage_manager.close()
        return df
    except Exception as e:
        logger.error("Error retrieving country group DataFrame: " + str(e))
        return pd.DataFrame()  # Return an empty DataFrame if an error occurs



async def update_base_file(df=None, blob_name=None):
    """
    Uploads a DataFrame as a CSV file to Azure Blob Storage.

    Args:
        df (pandas.DataFrame): The DataFrame to upload.
        blob_name (str): The name of the blob file in Azure Blob Storage.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    storage_manager = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=AZURE_STORAGE_CONNECTION_STRING,
        container_name=AZURE_STORAGE_CONTAINER_NAME,
        use_singleton=False
    )
    try:
        # Reset the index of the DataFrame
        df.reset_index(inplace=True)

        # Drop rows with missing values in the STANDARD_KEY_COLUMN
        df.dropna(subset=[STANDARD_KEY_COLUMN], inplace=True)

        # Check if the blob file already exists in Azure Blob Storage
        blob_exists = await storage_manager.check_blob_exists(
            blob_name=os.path.join(ROOT_FOLDER, 'output', 'access_all_data', 'base', blob_name)
        )

        # Create an empty DataFrame for the keys
        key_df = pd.DataFrame(columns=[STANDARD_KEY_COLUMN])

        # Get the country group DataFrame
        country_group_df = await country_group_dataframe()

        if blob_exists:
            logger.info(f"Base file {blob_name} exists. Updating...")
            # Download the base file as bytes and read it as a DataFrame
            base_file_bytes = await storage_manager.download(
                blob_name=os.path.join(ROOT_FOLDER, 'output', 'access_all_data', 'base', blob_name),
                dst_path=None
            )
            base_file_df = pd.read_csv(io.BytesIO(base_file_bytes))
            base_file_df = pd.DataFrame(columns=[STANDARD_KEY_COLUMN])
        else:
            logger.info("Base file does not exist. Creating...")
            base_file_df = pd.DataFrame(columns=[STANDARD_KEY_COLUMN])
            base_file_df[STANDARD_KEY_COLUMN] = country_group_df[STANDARD_KEY_COLUMN]

        # Filter base_file_df to include only rows with keys present in country_group_df
        base_file_df = base_file_df[base_file_df[STANDARD_KEY_COLUMN].isin(country_group_df[STANDARD_KEY_COLUMN])]

        # Select the keys from country_group_df that are not present in base_file_df
        key_df[STANDARD_KEY_COLUMN] = country_group_df[~country_group_df[STANDARD_KEY_COLUMN].isin(base_file_df[STANDARD_KEY_COLUMN])][STANDARD_KEY_COLUMN]

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
        #this will add a new int column with new indices
        #base_file_df.reset_index(inplace=True)

        # Define the destination path for the CSV file
        destination_path = os.path.join(ROOT_FOLDER, 'output', 'access_all_data', 'base', blob_name)

        await storage_manager.upload(
                    data=base_file_df.to_csv(encoding='utf-8'),
                    dst_path=destination_path,
                    overwrite=True,
                    content_type="text/csv"
        )

        await storage_manager.close()

    except Exception as e:
        await storage_manager.close()
        logger.error(f"Error uploading to blob: {e}")
        raise



if __name__ == "__main__":
    test_df = pd.read_csv("./BTI_PROJECT.csv")
    asyncio.run(update_base_file(df=test_df, blob_name="BTI_PROJECT.csv"))
    # asyncio.run(country_group_dataframe())
