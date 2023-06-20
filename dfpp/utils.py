import asyncio
import io
import logging
from tempfile import TemporaryDirectory
import pandas as pd
from dfpp.storage import AsyncAzureBlobStorageManager
from dfpp.constants import *

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
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=AZURE_STORAGE_CONNECTION_STRING,
        container_name=AZURE_STORAGE_CONTAINER_NAME,
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
        # await storage.close()
        return source_df
    except Exception as e:
        print(e)


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
    )
    country_lookup_bytes = await storage.download(COUNTRY_LOOKUP_CSV_PATH)
    country_code_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_code_lookup")
    print(country_code_df.head())
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
    year_columns = {}
    column_map = {}
    for column in columns:
        column_map[column] = column
    if column_substring is not None:
        print("THIS IS THE COLUMN SUBSTRING: " + column_substring)
        for column in columns:
            column_map[column] = column_map[column].replace(column_substring, "")
    elif col_prefix is not None or col_suffix is not None:
        if col_prefix is not None:
            for column in columns:
                column_map[column] = column_map[column].rsplit(col_prefix)[-1]
        if col_suffix is not None:
            for column in columns:
                column_map[column] = column_map[column].rsplit(col_suffix)[0]
    for column in column_map:
        try:
            year = int(float(column_map[column]))
            year_columns[year] = column
        except Exception as e:
            pass
    return year_columns


async def rename_indicator(indicator, year):
    return indicator + "_" + str(int(float(year)))


async def invert_dictionary(original_dictionary):
    inverted_dictionary = {}
    for dict_key in original_dictionary:
        inverted_dictionary[original_dictionary[dict_key]] = dict_key
    return inverted_dictionary


async def upload_to_blob_as_csv(df=None, blob_name=None):
    storage_manager = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=AZURE_STORAGE_CONNECTION_STRING,
        container_name=AZURE_STORAGE_CONTAINER_NAME,
        use_singleton=False
    )
    try:
        destination_path = os.path.join(ROOT_FOLDER, 'output', 'access_all_data', 'base', blob_name)
        with TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, blob_name)
            df.to_csv(temp_file)
            await storage_manager.upload(
                src_path=temp_file,
                dst_path=destination_path,
                overwrite=True,
                content_type="text/csv"
            )
        await storage_manager.close()
        return True
    except Exception as e:
        logger.error("Error uploading to blob: " + str(e))
        return False


if __name__ == "__main__":
    test_df = pd.read_csv("./BTI_PROJECT.csv")
    asyncio.run(upload_to_blob_as_csv(df=test_df, blob_name="BTI_PROJECT.csv"))
