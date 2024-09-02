import io
import logging
import os

import numpy as np
import pandas as pd

from dfpp.constants import STANDARD_KEY_COLUMN
from dfpp.storage import StorageManager
from dfpp.exceptions_logger import handle_exceptions

logger = logging.getLogger(__name__)


@StorageManager.with_storage_manager
def _get_population_data(storage_manager):
    population_path = os.path.join(storage_manager.utilities_path, "population.csv")
    popuation_data = await storage_manager.read_blob(path=population_path)
    population_df = pd.read_csv(io.BytesIO(popuation_data))

    long_population = population_df.melt(
        id_vars=["Alpha-3 code"],
        value_vars=population_df.select_dtypes("number"),
        value_name="totalpopulation_untp_year",
    )
    long_population["totalpopulation_untp"] = long_population["variable"].str.extract(
        r"(\D+)"
    )
    long_population["year"] = long_population["variable"].str.extract(r"(\d+)")
    long_population["year"] = long_population["year"].astype(int)
    return long_population


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