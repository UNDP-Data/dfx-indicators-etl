import io
import logging
import os
import warnings

import pandas as pd

from dfpp.constants import STANDARD_KEY_COLUMN
from dfpp.exceptions import TransformationError, TransformationWarning
from dfpp.storage import StorageManager
from dfpp.exceptions_logger import handle_exceptions
from dfpp.geo_utils import country_group_dataframe

logger = logging.getLogger(__name__)


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

