"""
A module responsible for publishing Data Futures Platform pipeline data sets

"""
import io
import logging
import os
from datetime import datetime
import asyncio
from typing import OrderedDict
import swifter
import numpy as np
import pandas as pd

from dfpp.constants import STANDARD_KEY_COLUMN
from dfpp.run_transform import read_indicators_config
from dfpp.storage import AsyncAzureBlobStorageManager
from dfpp.utils import country_group_dataframe

logger = logging.getLogger(__name__)
project = 'VaccineEquity'
output_data_type = 'timeseries'
PATH_TO_OUTPUT_CSV = os.path.join(os.environ["ROOT_FOLDER"], "output", "access_all_data", "output.csv")


async def read_base_files():
    """
    Read the base files from the data folder
    """
    base_file_names = []
    storage_manager = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
        container_name=os.environ["AZURE_STORAGE_CONTAINER_NAME"],
        use_singleton=False,
    )
    # Added `/` at the end of the base file as delimiter so as the top level folder does not appear in the results
    PATH_TO_BASE_FILES = os.path.join(
        os.environ["ROOT_FOLDER"], "output", "access_all_data", "base/"
    )
    base_files = await storage_manager.list_blobs(
        prefix=PATH_TO_BASE_FILES,
    )
    for file in base_files:
        base_file_names.append(file.name)
    await storage_manager.close()
    return base_file_names


async def update_and_get_output_csv():
    """
    Update the output csv file with the latest data
    """
    storage_manager = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
        container_name=os.environ["AZURE_STORAGE_CONTAINER_NAME"],
        use_singleton=False,
    )

    logger.info("Reading current stored output.csv file...")
    output_df = pd.read_csv(io.BytesIO(await storage_manager.download(blob_name=PATH_TO_OUTPUT_CSV)))
    logger.info("Starting reading base files...")
    base_files = await read_base_files()

    async def read_base_df(base_file):
        logger.info(f"Reading base file {base_file} to a dataframe...")
        base_file_df = pd.read_csv(io.BytesIO(await storage_manager.download(blob_name=base_file)))
        return base_file_df, base_file

    basefile_reading_tasks = []
    for file in base_files:
        asyncio.create_task(read_base_df(file))
        basefile_reading_tasks.append(read_base_df(file))
    base_dfs = await asyncio.gather(*basefile_reading_tasks)
    for base_df, name in base_dfs:
        columns_to_update = set(output_df.columns.to_list()).intersection(set(base_df.columns.to_list()))
        columns_to_add = set(base_df.columns.to_list()).difference(set(output_df.columns.to_list()))
        logger.info(f"Updating {len(columns_to_update)} columns in the output dataframe with data from {name}...")
        output_df.update(base_df[list(columns_to_update)])
        logger.info(f"Adding {len(columns_to_add)} columns to the output dataframe from {name}...")
        output_df = output_df.join(base_df[list(columns_to_add)])
        # update indicator metadata in the indicator config with the information on when the data was last updated
    logger.info("Uploading the updated output csv to Azure Blob Storage...")
    await storage_manager.upload(
        data=output_df.to_csv(index=False, encoding='utf-8'),
        dst_path=PATH_TO_OUTPUT_CSV,
        overwrite=True,
        content_type='text/csv',
    )
    await storage_manager.close()
    return output_df


async def get_time_series_cols(indicator_cfgs: list= None,
                               columns: list = None):
    time_series_mapping = {}
    time_series_cols = []
    indicators = [indicator["indicator"]["indicator_id"] for indicator in indicator_cfgs]
    for indicator in indicators:
        indicator_cols = []
        for col in columns:
            if "_".join(col.rsplit("_")[:-1]) == indicator:
                indicator_cols.append(col)
        time_series_mapping[indicator] = indicator_cols
        time_series_cols += indicator_cols
    return time_series_mapping, time_series_cols


async def map_datatype(value=None):
    if value == "text":
        return "str"
    if value == "float" or value == "int":
        return "float"
    else:
        return "str"


async def process_time_series_data(storage_manager: AsyncAzureBlobStorageManager,
                                   dataframe: pd.DataFrame = None,
                                   indicator_cfgs: list = None) -> pd.DataFrame:
    """
    Preprocesses the input DataFrame for publishing.

    Args:
        dataframe (pd.DataFrame): The input DataFrame to preprocess.
        storage_manager (AsyncAzureBlobStorageManager): The storage manager object for accessing Azure Blob Storage.
        indicator_cfgs (list): A list of indicator configurations.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.

    Raises:
        Exception: If there is an error during type conversion or rounding.

    Note:
        This function assumes the availability of the following objects/functions:
        - read_indicators_config(): A function that returns a list of indicator configurations.
        - get_time_series_cols(storage_manager, columns): An asynchronous function that returns time series mapping
                                                         and time series columns.


    """
    logger.info("Starting preprocessing of time series data...")
    type_map = {}  # Mapping of column names to their respective data types
    rounding_map = {}  # Mapping of column names to rounding decimal places

    # Map indicator IDs to their respective data types
    datatype_mapping = {}
    for indicators_cfg in indicator_cfgs:
        datatype_mapping[indicators_cfg['indicator']['indicator_id']] = indicators_cfg['indicator']['data_type']

    # Get time series mapping and columns
    logger.info("Getting time series mapping and columns...")
    time_series_mapping, time_series_cols = await get_time_series_cols(indicator_cfgs=indicator_cfgs,
                                                                       columns=dataframe.columns.to_list())

    # Determine valid columns for preprocessing
    valid_cols = time_series_cols + [STANDARD_KEY_COLUMN]
    dataframe = dataframe[dataframe.columns.intersection(valid_cols)]

    # Generate type_map and rounding_map based on datatype_mapping
    logger.info("Generating type map and rounding map...")
    for key, value in datatype_mapping.items():
        for time_series_col in time_series_mapping[key]:
            type_map[time_series_col] = await map_datatype(value)
            if value == 'float':
                rounding_map[time_series_col] = 2
            elif value == 'int':
                rounding_map[time_series_col] = 0

    try:
        dataframe = dataframe.astype(type_map)  # Convert columns to specified data types
    except Exception as e:
        logger.error("Failed to change type", e)

    try:
        dataframe = dataframe.round(rounding_map)  # Round numeric columns to specified decimal places
    except Exception as e:
        logger.error("Failed to round", e)

    dataframe.replace("None", np.nan, inplace=True)  # Replace 'None' values with NaN
    logger.info("Starting to generate indicator data")
    dataframe = await generate_indicator_data(dataframe=dataframe, indicator_cfgs=indicator_cfgs)
    return dataframe


# async def generate_indicator_data(dataframe: pd.DataFrame = None, indicator_cfgs: list = None,
#                                   timeseries_cols: list = None):
#     logger = logging.getLogger(__name__)
#     logger.setLevel(logging.DEBUG)
#     handler = logging.StreamHandler()
#     handler.setLevel(logging.DEBUG)
#     formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#     handler.setFormatter(formatter)
#     logger.addHandler(handler)
#
#     async def generate_indicator_timeseries_data(row: pd.Series = None, columns: list = None):
#         """
#         Generates indicator data based on the provided row and final output columns.
#
#         Args:
#             row (pd.Series): The input row.
#             columns (list): The list of final output columns.
#
#         Returns:
#             list: A list of dictionaries representing the generated indicator data.
#
#         """
#         storage_manager = await AsyncAzureBlobStorageManager.create_instance(
#             connection_string=os.environ.get('AZURE_STORAGE_CONNECTION_STRING'),
#             container_name=os.environ.get('AZURE_STORAGE_CONTAINER_NAME'),
#         )
#
#         data = []
#         indicators = {}
#         indicator_list = await storage_manager.list_indicators()
#         for column in columns:
#             try:
#                 indicator = "_".join(column.rsplit("_")[:-1])
#                 indicator_year = (column.rsplit("_")[-1])
#                 indicator_year = float(indicator_year)
#                 if indicator_year < 1980:
#                     continue
#                 if (indicator in indicator_list) and not (pd.isnull(row[column])):
#                     if indicator not in indicators:
#                         indicators[indicator] = []
#                     indicators[indicator].append({"year": int(float((column.rsplit("_")[-1]))), "value": row[column]})
#             except Exception as e:
#                 logger.warning("Error processing column %s: %s", column, str(e))
#         indicators = dict(OrderedDict[sorted(indicators.items())])
#         for indicator in indicators:
#             available_years = []
#             for single_dict in indicators[indicator]:
#                 available_years.append((single_dict["year"]))
#             available_years.sort()
#             indicators[indicator].sort(key=lambda x: x["year"])
#
#             current_indicator = filter(lambda x: x['indicator']['indicator_id'] == indicator, indicator_cfgs)
#             current_indicator = list(current_indicator)[0]
#
#             data.append({
#                 "indicator": current_indicator["indicator_name"],
#                 "yearlyData": indicators[indicator]
#             })
#         return data
#
#     columns_list = dataframe.columns.to_list()
#
#     async def process_row(row: pd.Series, columns: list):
#         return await generate_indicator_timeseries_data(row=row, columns=columns)
#
#     tasks = []
#     for _, row in dataframe.iterrows():
#         task = asyncio.create_task(process_row(row, columns_list))
#         tasks.append(task)
#
#     indicator_results = await asyncio.gather(*tasks)
#
#     dataframe['indicators'] = indicator_results
#     added_indicators_list = list(set(timeseries_cols) & set(dataframe.columns.to_list()))
#     dataframe.drop(added_indicators_list, axis=1, inplace=True)
#     dataframe.set_index(STANDARD_KEY_COLUMN, inplace=True)
#     country_dataframe = await country_group_dataframe()
#     country_dataframe.set_index(STANDARD_KEY_COLUMN, inplace=True)
#     columns_set = set(dataframe.columns)
#     country_list = country_dataframe.drop(["Alpha-2 code", "Numeric code", "Development classification", "Group 3"],
#                                           axis=1)
#     dataframe = dataframe.join(country_list)
#     dataframe.reset_index(inplace=True)
#     dataframe = dataframe[[STANDARD_KEY_COLUMN] + country_dataframe.columns.to_list() + columns_list]
#     # generate json file and upload to blob
#     logger.debug("Generated dataframe:\n%s", dataframe.head())
#     return dataframe


async def generate_indicator_data(dataframe: pd.DataFrame = None, indicator_cfgs: list = None,
                                  timeseries_cols: list = None):
    async def generate_indicator_timeseries_data(df_row=None, columns: list = None):
        """
        Generates indicator data based on the provided row and final output columns.

        Args:
            df_row (pd.Series): The input row.
            columns (list): The list of final output columns.

        Returns:
            list: A list of dictionaries representing the generated indicator data.

        """
        # logger.info("Generating indicator data for row: %s", row[STANDARD_KEY_COLUMN])

        data = []
        indicators = {}
        indicator_list = [indicator["indicator"]["indicator_id"] for indicator in indicator_cfgs]
        for column in columns:
            try:
                indicator = "_".join(column.rsplit("_")[:-1])
                indicator_year = (column.rsplit("_")[-1])
                indicator_year = float(indicator_year)
                if indicator_year < 1980:  # TODO: make this configurable, or use the indicator config
                    continue
                if (indicator in indicator_list) and not (pd.isnull(row[column])):
                    if indicator not in indicators:
                        indicators[indicator] = []
                    indicators[indicator].append({"year": int(float((column.rsplit("_")[-1]))), "value": row[column]})
            except Exception as e:
                logger.warning("Error processing column %s: %s", column, str(e))
        print(indicators)
        # indicators = sorted(indicators.items())
        # indicators = dict(OrderedDict[sorted(indicators.items())])
        # for indicator in indicators:
        #     available_years = []
        #     for single_dict in indicators[indicator]:
        #         available_years.append((single_dict["year"]))
        #     available_years.sort()
        #     indicators[indicator].sort(key=lambda x: x["year"])
        #
        #     current_indicator = filter(lambda x: x['indicator']['indicator_id'] == indicator, indicator_cfgs)
        #     current_indicator = list(current_indicator)[0]
        #
        #     data.append({
        #         "indicator": current_indicator["indicator_name"],
        #         "yearlyData": indicators[indicator]
        #     })
        return data

    columns_list = dataframe.columns.to_list()
    # dataframe['indicators'] = []
    tasks = []
    for row in dataframe.iterrows():
        task = asyncio.create_task(generate_indicator_timeseries_data(df_row=row, columns=columns_list))
        print(task)
        # await generate_indicator_timeseries_data(df_row=row, columns=columns_list)
    #     task = asyncio.create_task(generate_indicator_timeseries_data(row=row, columns=columns_list))
    #     tasks.append(task)
    # dataframe['indicators'] = await asyncio.gather(*tasks)
    # dataframe['indicators'] = dataframe.swifter.apply(lambda row: generate_indicator_timeseries_data(row=row, columns=columns_list), axis=1)
    added_indicators_list = list(set(timeseries_cols) & set(dataframe.columns.to_list()))
    print(added_indicators_list)
    dataframe.drop(added_indicators_list, axis=1, inplace=True)
    dataframe.set_index(STANDARD_KEY_COLUMN, inplace=True)
    country_dataframe = await country_group_dataframe()
    country_dataframe.set_index(STANDARD_KEY_COLUMN, inplace=True)
    columns_set = set(dataframe.columns)
    country_list = country_dataframe.drop(["Alpha-2 code", "Numeric code", "Development classification", "Group 3"],
                                          axis=1)
    dataframe = dataframe.join(country_list)
    dataframe.reset_index(inplace=True)
    dataframe = dataframe[[STANDARD_KEY_COLUMN] + country_dataframe.columns.to_list() + columns_list]
    # generate json file and upload to blob
    print(dataframe.head())
    return dataframe


async def process_latest_data(
        storage_manager: AsyncAzureBlobStorageManager,
        dataframe: pd.DataFrame = None,
        indicator_cfgs: list = None,
):
    #     indicator_list = [indicator['indicator']['indicator_id'] for indicator in indicator_cfgs]
    #     async def generate_latest_data(row: pd.Series = None):
    #         d = []
    #         for v in indicator_list:
    #             if v in row:
    #                 d.append({
    #                     'indicator': indicator_df.loc[v]["indicator_name"],
    #                     'value': row[v]
    #                     # 'labelExtra':indicator_df.loc[v]["Display Name"]
    #                 })
    #         return d
    pass


async def publish():
    """
    Publish the data to the Data Futures Platform
    :return:
    """
    storage_manager = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
        container_name=os.environ["AZURE_STORAGE_CONTAINER_NAME"],
        use_singleton=False,
    )

    # Read indicator configurations
    indicator_cfgs = await read_indicators_config()

    # update and get the updated output csv
    # TODO: Reading from local file for now
    # output_df = await update_and_get_output_csv()
    output_df = pd.read_csv('/home/thuha/Downloads/output.csv')
    logger.info("Output dataframe:")
    if output_data_type == "timeseries":
        indicator_cfgs = list(filter(lambda x: x['indicator']['indicator_name'] == "Employement outside the Formal Sector by sex and status in employment (thousands), Total, Self-employed",indicator_cfgs))
        output_df = await process_time_series_data(
            storage_manager=storage_manager,
            dataframe=output_df,
            indicator_cfgs=indicator_cfgs
        )
    elif output_data_type == "latest":
        output_df = await process_latest_data(
            storage_manager=storage_manager,
            dataframe=output_df,
            indicator_cfgs=indicator_cfgs
        )
    await storage_manager.close()
