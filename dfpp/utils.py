import asyncio
import io
import os
import pandas as pd
from dfpp.storage import AsyncAzureBlobStorageManager


KEY_COL = "Alpha-3 code"
COUNTRY_COL = "Country"
country_lookup_path = "DataFuturePlatform/pipeline/config/utilities/country_lookup.xlsx"


async def add_country_code(source_df):
    """
    Adds country codes to a DataFrame based on a country lookup table.

    Args:
        source_df (pandas.DataFrame): The source DataFrame to add country codes to.

    Returns:
        pandas.DataFrame: The source DataFrame with country codes added.

    """
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
        container_name=os.getenv('CONTAINER_NAME'),
    )
    country_lookup_bytes = await storage.download(country_lookup_path)
    country_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_lookup")

    country_df.set_index("Country", inplace=True)
    country_df.dropna(subset=[KEY_COL], inplace=True)
    source_df = source_df.join(country_df[KEY_COL])
    source_df.reset_index(inplace=True)
    await storage.close()
    return source_df


async def add_region_code(source_df, region_name_col, region_key_col=None):
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
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
        container_name=os.getenv('CONTAINER_NAME'),
    )
    country_lookup_bytes = await storage.download(country_lookup_path)
    region_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="region_lookup")
    region_df = region_df.drop_duplicates(subset=["Region"], keep='last')
    region_df.dropna(subset=["Region"], inplace=True)

    if "Region" != region_df.index.name:
        region_df['Region'] = region_df['Region'].str.replace(' ', '')
        region_df = region_df.drop_duplicates(subset=["Region"], keep='last')
        region_df.set_index("Region", inplace=True)
        region_df.dropna(subset=[KEY_COL], inplace=True)

    if region_name_col != source_df.index.name:
        source_df[region_name_col] = source_df[region_name_col].str.replace(' ', '')
        source_df.set_index(region_name_col, inplace=True)

    if region_key_col is None:
        col_list = source_df.columns.to_list()
        if KEY_COL in col_list:
            source_df.update(region_df[KEY_COL])

        else:
            source_df = source_df.join(region_df[KEY_COL])

    else:
        region_df.rename(columns={KEY_COL: region_key_col}, inplace=True)
        source_df.update(region_df[region_key_col])
        source_df.rename(columns={region_key_col: KEY_COL}, inplace=True)

    source_df.reset_index(inplace=True)
    source_df.dropna(subset=[KEY_COL], inplace=True)
    await storage.close()
    return source_df


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
        connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
        container_name=os.getenv('CONTAINER_NAME'),
    )
    country_lookup_bytes = await storage.download(country_lookup_path)
    country_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_lookup")
    country_df = country_df.rename(columns={'Country': country_col})
    await storage.close()
    return pd.merge(source_df, country_df[['Alpha-3 code', country_col]], on=[country_col], how='left')


async def fix_iso_country_codes(df, col, sourceId=None):
    """
    Fixes ISO country codes in a DataFrame based on a country code lookup table.

    Args:
        df (pandas.DataFrame): The DataFrame to fix the ISO country codes in.
        col (str): The name of the column containing ISO country codes in `df`.
        sourceId (str, optional): The source ID for which to fix the ISO country codes.
            If provided, only rows with a matching source ID will be updated. Defaults to None.

    Returns:
        None: The function modifies the provided DataFrame in place.

        """
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
        container_name=os.getenv('CONTAINER_NAME'),
    )
    country_lookup_bytes = await storage.download(country_lookup_path)
    country_code_df = pd.read_excel(io.BytesIO(country_lookup_bytes), sheet_name="country_code_lookup")
    for index, row in country_code_df.iterrows():
        print(sourceId, row.get("Only For Souce ID"), row['ISO 3 Code'], row['UNDP ISO 3 Code'])
        if sourceId is None or sourceId == row.get("Only For Souce ID"):
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
    source_df = add_region_code(source_df, temp_col, iso3_col)
    source_df[iso3_col] = source_df[KEY_COL]
    drop_list = [temp_col]
    if iso3_col != KEY_COL:
        drop_list.append(KEY_COL)
    return source_df.drop(columns=drop_list)


if __name__ == "__main__":
    df = pd.DataFrame([{'Country': 'United States of America'}])
    new_df = asyncio.run(add_country_code(df))
    print(new_df)