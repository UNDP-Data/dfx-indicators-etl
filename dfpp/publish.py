"""
A module responsible for publishing Data Futures Platform pipeline data sets

"""
import io
import json
import logging
import os
import asyncio
from typing import List

import numpy as np
import pandas as pd

from dfpp.constants import STANDARD_KEY_COLUMN, OUTPUT_FOLDER
from dfpp.storage import  StorageManager
from dfpp.utils import country_group_dataframe, region_group_dataframe

AREA_TYPES = ['countries', 'regions']
logger = logging.getLogger(__name__)
# project = 'vaccine_equity'
project = 'access_all_data'
output_data_type = 'timeseries'


# output_data_type = 'latestavailabledata'


async def update_and_get_output_csv(storage_manager: StorageManager,
                                    area_type: str,
                                    indicator_cfgs: List=None,
                                    project='access_all_data'
                                    ):
    """
    Update the output csv file with the latest data
    """
    output_csv_file_path = os.path.join(OUTPUT_FOLDER, project, f'output_{area_type}.csv')
    logger.info(f"Reading current stored {output_csv_file_path} file...")
    if not await storage_manager.check_blob_exists(output_csv_file_path):
        output_df = await country_group_dataframe()
        output_df = output_df[[STANDARD_KEY_COLUMN]]
        output_df.set_index(STANDARD_KEY_COLUMN, inplace=True)
        logger.info(f'Creating {output_csv_file_path}')
        await storage_manager.upload(dst_path=output_csv_file_path,data=output_df.to_csv(encoding='utf-8'))
        output_df.reset_index(STANDARD_KEY_COLUMN, inplace=True)
    else:
        output_df = pd.read_csv(
        io.BytesIO(
            await storage_manager.download(blob_name=output_csv_file_path)))


    # if area_type == 'countries':
    #
    # elif area_type == 'region':
    #     output_df = pd.read_csv(io.BytesIO(await storage_manager.download(blob_name=os.path.join(OUTPUT_FOLDER, project, 'output_regions.csv'))))

    logger.info("Starting reading base files...")
    if not indicator_cfgs:
        base_files_list = await storage_manager.list_base_files()
    else:
        base_files_list = []
        for indicator_cfg in indicator_cfgs:
            base_file_path = os.path.join(storage_manager.OUTPUT_PATH, project, 'base', f"{indicator_cfg['indicator']['source_id']}.csv")
            assert await storage_manager.check_blob_exists(base_file_path), f'{base_file_path} doe not exist'
            base_files_list.append(
                base_file_path

            )



    async def read_base_df(base_file):
        logger.info(f"Reading base file {base_file.split('/')[-1]} to a dataframe...")
        base_file_df = pd.read_csv(io.BytesIO(await storage_manager.download(blob_name=base_file)))

        return base_file_df, base_file

    basefile_reading_tasks = []

    for file in base_files_list:
        asyncio.create_task(read_base_df(file))
        basefile_reading_tasks.append(read_base_df(file))
    base_dfs = await asyncio.gather(*basefile_reading_tasks)
    for base_df, name in base_dfs:

        columns_to_update = list(set(output_df.columns.to_list()).intersection(set(base_df.columns.to_list())))
        columns_to_add = list(set(base_df.columns.to_list()).difference(set(output_df.columns.to_list()))) + [STANDARD_KEY_COLUMN]
        # as no index was set the common columns needs to be removed from update list
        columns_to_update.remove(STANDARD_KEY_COLUMN)
        '''
        Indices are not necessary, in fact if you set na index it will  be excluded on merge operation 
        '''
        if columns_to_update:
            logger.info(f"Updating {len(columns_to_update)} columns in the output dataframe with data from {name}...")
            output_df.update(base_df[columns_to_update])

        if columns_to_add:
            logger.info(f"Adding {len(columns_to_add)} columns to the output dataframe from {name}...")
            output_df = pd.merge(output_df, base_df[columns_to_add], on=STANDARD_KEY_COLUMN)


        # update indicator metadata in the indicator config with the information on when the data was last updated
    logger.info(f"Uploading {output_csv_file_path} to Azure Blob Storage...")
    await storage_manager.upload(
        data=output_df.to_csv(index=False).encode('utf-8'),
        dst_path=output_csv_file_path,
        overwrite=True,
        content_type='text/csv',
    )

    return output_df#, base_df.columns.to_list()


async def get_time_series_cols(indicator_cfgs: list = None,
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
    elif value in ("float", "int"):
        return "float"
    else:
        return "str"


async def generate_indicator_data(
        storage_manager: StorageManager,
        dataframe: pd.DataFrame = None,
        indicator_cfgs: list = None,
        timeseries_cols: list = None,
        area_type: str = None,
):
    indicator_list = [indicator["indicator"]["indicator_id"] for indicator in indicator_cfgs]

    async def generate_indicator_timeseries_data(df_row: pd.Series = None, columns: list = None):
        """
        Generates indicator data based on the provided row and final output columns.

        Args:
            df_row (pd.Series): The input row.
            columns (list): The list of final output columns.

        Returns:
            list: A list of dictionaries representing the generated indicator data.

        """

        data = []
        indicators = {}

        for column in columns:
            if column == STANDARD_KEY_COLUMN:
                continue
            try:
                indicator = "_".join(column.rsplit("_")[:-1])
                indicator_year = (column.rsplit("_")[-1])
                indicator_year = float(indicator_year)
                if indicator_year < 1980:  # TODO: make this configurable, or use the indicator config
                    continue
                if (indicator in indicator_list) and not (pd.isnull(df_row.loc[column])):
                    if indicator not in indicators:  # if indicator not in indicators, add an empty list to it
                        indicators[indicator] = []
                    indicators[indicator].append(
                        {"year": int(float((column.rsplit("_")[-1]))), "value": df_row[column]})
            except Exception as e:
                logger.warning("Error processing column %s: %s", column, str(e))
        indicators = dict(sorted(indicators.items()))
        for indicator in indicators:
            available_years = []
            for single_dict in indicators[indicator]:
                available_years.append((single_dict["year"]))
            available_years.sort()
            indicators[indicator].sort(key=lambda x: x["year"])

            current_indicator = filter(lambda x: x['indicator']['indicator_id'] == indicator, indicator_cfgs)
            current_indicator = list(current_indicator)[0]
            data.append({
                "indicator": current_indicator['indicator']["indicator_name"],
                "yearlyData": indicators[indicator]
            })
        return data

    columns_list = dataframe.columns.to_list()
    tasks = []
    for row in dataframe.iterrows():
        task = asyncio.create_task(generate_indicator_timeseries_data(df_row=row[1], columns=columns_list))
        tasks.append(task)
    dataframe['indicators'] = await asyncio.gather(*tasks)
    # dataframe['indicators'] = dataframe.swifter.apply(lambda row: asyncio.run(generate_indicator_timeseries_data(df_row=row[1], columns=columns_list)), axis=1)
    added_indicators_columns = list(set(timeseries_cols) & set(dataframe.columns.to_list()))

    dataframe.drop(added_indicators_columns, axis=1, inplace=True)
    if area_type == "countries":
        country_dataframe = await country_group_dataframe()
    else:
        country_dataframe = await region_group_dataframe()
    dataframe.set_index(STANDARD_KEY_COLUMN, inplace=True)
    country_dataframe.set_index(STANDARD_KEY_COLUMN, inplace=True)

    # find if dataframe contains any row with any of the regions codes
    # region_codes = country_dataframe.index.to_list()
    # if any(dataframe.index.isin(region_codes)):
    #     print("Dataframe contains region codes")

    columns_to_drop = ["Alpha-2 code", "Numeric code", "Development classification", "Group 3"]
    country_dataframe = country_dataframe.drop(columns_to_drop, axis=1)

    # Join country_dataframe with the dataframe
    dataframe = dataframe.join(country_dataframe, on=STANDARD_KEY_COLUMN)
    dataframe.reset_index(inplace=True)
    # dataframe = dataframe[[STANDARD_KEY_COLUMN] + country_dataframe.columns.to_list() + columns_list]
    # Upload json file to blob
    await storage_manager.upload(
        dst_path=os.path.join(OUTPUT_FOLDER, project, f"output_{area_type}.json"),
        data=dataframe.to_json(orient="records").encode("utf-8"),
        content_type="application/json",
        overwrite=True,
    )
    # upload minified json file to blob
    await storage_manager.upload(
        dst_path=os.path.join(OUTPUT_FOLDER, project, f"output_{area_type}_minified.json"),
        data=dataframe.to_json(orient="records", indent=False).encode("utf-8"),
        content_type="application/json",
        overwrite=True,
    )
    upload_per_country_task = []
    for index in dataframe.index:
        row = dataframe.loc[index]
        task = asyncio.create_task(
            storage_manager.upload(
                dst_path=os.path.join(OUTPUT_FOLDER, project, f"Output{area_type.capitalize()}",
                                      row[STANDARD_KEY_COLUMN] + ".json"),
                data=json.dumps(row.to_dict()).encode("utf-8"),
                content_type="application/json",
                overwrite=True,
            )
        )
        upload_per_country_task.append(task)
    await asyncio.gather(*upload_per_country_task)

    return dataframe


async def process_time_series_data(
        storage_manager: StorageManager,
        dataframe: pd.DataFrame = None,
        indicator_cfgs: list = None,
        area_type: str = None) -> pd.DataFrame:
    """
    Preprocesses the input DataFrame for publishing.

    Args:
        dataframe (pd.DataFrame): The input DataFrame to preprocess.
        storage_manager (AsyncAzureBlobStorageManager): The storage manager object for accessing Azure Blob Storage.
        indicator_cfgs (list): A list of indicator configurations.
        area_type (str): The area type of the input data.
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
    dataframe = await generate_indicator_data(
        storage_manager=storage_manager,
        dataframe=dataframe,
        indicator_cfgs=indicator_cfgs,
        timeseries_cols=time_series_cols,
        area_type=area_type)
    logger.info("Finished generating indicator data")
    return dataframe


async def process_latest_data(
        storage_manager: StorageManager,
        dataframe: pd.DataFrame = None,
        indicator_cfgs: list = None,
        area_type: str = None
):
    logger.info("Starting preprocessing of latest data...")
    indicator_list = [indicator['indicator']['indicator_id'] for indicator in indicator_cfgs]
    datatype_mapping = {}
    logger.info("Generating datatype mapping...")
    for indicators_cfg in indicator_cfgs:
        datatype_mapping[indicators_cfg['indicator']['indicator_id']] = indicators_cfg['indicator']['data_type']

    async def generate_latest_data(df_row: pd.Series = None):
        data = []
        for indicator in indicator_list:
            if indicator in df_row:
                data.append({
                    "indicator": indicator,
                    "value": df_row[indicator]
                })
        return data

    valid_cols = list(set(indicator_list)) + ["Country or Area", "Alpha-2 code", "Alpha-3 code", "Numeric code",
                                              "Latitude (average)", "Longitude (average)", "Group 1", "Group 2",
                                              "Group 3", "LDC", "LLDC", "SIDS", "Development classification",
                                              "Income group"]
    dataframe = dataframe[dataframe.columns.intersection(valid_cols)]
    for key, value in datatype_mapping.items():
        if key in dataframe.columns:
            try:
                dataframe = dataframe.astype({key: await map_datatype(value)})
            except Exception as e:
                logger.error("Failed to change type", e)
            finally:
                try:
                    if value == 'float':
                        dataframe = dataframe.round({key: 2})
                    elif value == 'int':
                        dataframe = dataframe.round({key: 0})
                except Exception as e:
                    logger.error("Failed to round", e)
    dataframe.replace("None", np.nan, inplace=True)
    tasks = []
    for row in dataframe.iterrows():
        task = asyncio.create_task(generate_latest_data(row[1]))
        tasks.append(task)
    dataframe['data'] = await asyncio.gather(*tasks)
    # dataframe = dataframe.swifter.apply(lambda x: asyncio.run(generate_latest_data(x)), axis=1, result_type='expand')
    added_indicators_columns = list(set(indicator_list) & set(dataframe.columns.to_list()))
    dataframe.drop(added_indicators_columns, axis=1, inplace=True)
    if area_type == "countries":
        country_dataframe = await country_group_dataframe()
    else:
        country_dataframe = await region_group_dataframe()
        print(country_dataframe.head())
    dataframe = dataframe.merge(country_dataframe)

    # Upload json file to blob
    await storage_manager.upload(
        dst_path=os.path.join(OUTPUT_FOLDER, project, f"output_{area_type}.json"),
        data=dataframe.to_json(orient="records").encode("utf-8"),
        content_type="application/json",
        overwrite=True,
    )
    # upload minified json file to blob
    await storage_manager.upload(
        dst_path=os.path.join(OUTPUT_FOLDER, project, f"output_{area_type}_minified.json"),
        data=dataframe.to_json(orient="records", indent=False).encode("utf-8"),
        content_type="application/json",
        overwrite=True,
    )
    upload_tasks = []

    for index in dataframe.index:
        row = dataframe.iloc[index]
        upload_tasks.append(asyncio.create_task(
            storage_manager.upload(
                dst_path=os.path.join(OUTPUT_FOLDER, project, f'Output{area_type.capitalize()}',
                                      f"{row['Alpha-3 code']}.json"),
                data=json.dumps(row.to_dict()).encode("utf-8"),
                content_type="application/json",
                overwrite=True,
            )
        ))
    await asyncio.gather(*upload_tasks)

    return dataframe


async def generate_output_per_indicator(storage_manager: StorageManager, dataframe: pd.DataFrame = None,
                                        indicator_cfgs: list = None):
    """
        Generate output JSON files per indicator and upload them to Azure Blob Storage.

        Args:
            storage_manager (StorageManager): An instance of the StorageManager
                class used for uploading files to Azure Blob Storage.
            dataframe (pd.DataFrame, optional): The input DataFrame containing indicators data (output_df).
                Defaults to None.
            indicator_cfgs (List, optional): A list of indicator configurations.
                Defaults to None.

        Raises:
            Exception: Any exceptions that occur during the execution of the function.

        Returns:
            None
    """

    # Get the list of columns in the DataFrame
    columns_list = dataframe.columns.to_list()

    # Get the unique indicators present in the columns
    indicators_in_output = list(set(["_".join(column.split("_")[:-1]) for column in columns_list]))

    # Get the country dataframe
    country_dataframe = await country_group_dataframe()
    country_dataframe = country_dataframe[["Alpha-3 code", "Country or Area"]]

    # Merge the country dataframe with the input dataframe
    dataframe = dataframe.merge(country_dataframe)

    # List to store the upload tasks
    upload_tasks = []

    # Filter the indicator configurations based on indicators present in the output
    indicator_cfgs_in_output = list(
        filter(lambda ind: ind['indicator']['indicator_id'] in indicators_in_output, indicator_cfgs))

    # Process each indicator in the output
    for indicator in indicators_in_output:
        # Get the years associated with the indicator
        indicator_years = list(set([column.split("_")[-1] for column in columns_list if column.startswith(indicator)]))

        # Filter out valid years (4 digits)
        valid_years = [year for year in indicator_years if year.isdigit() and len(year) == 4]

        # Skip if no valid years found
        if len(valid_years) == 0:
            continue

        # Sort the indicator years
        indicator_years = sorted(valid_years)

        # Get the current indicator configuration
        current_indicator_list = list(
            filter(lambda ind: ind['indicator']['indicator_id'] == indicator, indicator_cfgs_in_output))

        # Proceed if a single indicator configuration is found
        if len(current_indicator_list) == 1:
            # Get indicator name
            indicator_name = current_indicator_list[0]['indicator']['indicator_name']

            # Prepare the indicator data
            indicator_data = {
                "indicator": indicator_name,
                "country_data": [
                    {
                        "Alpha-3 code": row["Alpha-3 code"],
                        "Country or Area": row["Country or Area"],
                        "data": [
                            {"year": int(year), "value": row[f"{indicator}_{year}"]}
                            for year in indicator_years
                            if not pd.isna(row[f"{indicator}_{year}"]) and int(year) >= 1980
                        ]
                    }
                    for index, row in dataframe.iterrows()
                ]
            }

            # Create an upload task for the indicator data
            upload_tasks.append(asyncio.create_task(
                storage_manager.upload(
                    dst_path=os.path.join(OUTPUT_FOLDER, project, "OutputIndicators", f"{indicator}.json"),
                    data=json.dumps(indicator_data).encode("utf-8"),
                    content_type="application/json",
                    overwrite=True,
                )
            ))

    # Wait for all upload tasks to complete
    await asyncio.gather(*upload_tasks)


async def publish(indicator_ids: list,
                    indicator_id_contain_filter: str = None,
                  project=None):
    """
    Publish the data to the Data Futures Platform
    :return:
    """
    assert project not in ['', None], f'Invalid project={project}.'

    if indicator_ids:
        logger.info(f"Starting to publish {len(indicator_ids)} indicators")

    async with StorageManager() as storage_manager:
        logger.info("Connected to Azure Blob Storage")
        logger.info("Starting to read indicator configurations...")


        indicator_cfgs = await storage_manager.get_indicators_cfg(
            indicator_ids=indicator_ids,
            contain_filter=indicator_id_contain_filter
        )

        for area_type in AREA_TYPES:

            '''
                CURRENTLY
                1. got a indicator cfg
                2. update output.csv with data and cols from indicator and upload the big CSV to azure 
                3. create JSON putput per indicator and upload  to azure
                4. create JSON output per AREA_TYPES and upload to azure
                
                
                DESIRED
                for indicator_cfg in indicators:
                    1. export each indicator to postgresql
                    2. optionally export to JSON
            '''


            # update and get the updated output csv
            # this SHOULD not be necessary


            output_df = await update_and_get_output_csv(
                storage_manager=storage_manager,
                area_type=area_type,
                indicator_cfgs=indicator_cfgs
            )
            # output_df = pd.read_csv('/home/thuha/Downloads/output.csv')
            await generate_output_per_indicator(storage_manager=storage_manager, dataframe=output_df,
                                                indicator_cfgs=indicator_cfgs)
            if output_data_type == "timeseries":
                output_df = await process_time_series_data(
                    storage_manager=storage_manager,
                    dataframe=output_df,
                    indicator_cfgs=indicator_cfgs,
                    area_type=area_type
                )

            # elif output_data_type == "latestavailabledata":
            #     output_df = await process_latest_data(
            #         storage_manager=storage_manager,
            #         dataframe=output_df,
            #         indicator_cfgs=indicator_cfgs,
            #         area_type=value
            #     )
            #




        logger.info("Finished publishing of data")
