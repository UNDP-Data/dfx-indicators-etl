import json
import os
import asyncio
import logging
from typing import List
import numpy as np
from dfpp.storage import StorageManager
from dfpp import preprocessing
from dfpp.utils import chunker
# This is importing all transform functions from transform_functions.py. DO NOT REMOVE EVEN IF IDE SAYS IT IS UNUSED
from dfpp import transform_functions
from io import StringIO
from traceback import print_exc

logger = logging.getLogger(__name__)


async def read_source_file_for_indicator(indicator_source: str = None):
    """
    Read the source file for a specific indicator asynchronously.

    :param indicator_source: The name of the indicator source.
    :type indicator_source: str
    :return: A tuple containing the data from the source file and its configuration.
    :rtype: tuple
    :raises Exception: If the indicator source is not specified or the source config file is not found.

    The function reads the source file associated with a specific indicator from Azure Storage asynchronously.
    It first checks if the indicator source is specified. Then, it reads the configuration file for the indicator
    from the storage and determines the file format of the source file. It proceeds to download the source file
    from the storage and returns the data along with the source configuration.

    Note: The function assumes the presence of a StorageManager class for interacting with Azure Storage.
    """

    # Ensure the indicator source is provided
    assert indicator_source is not None, 'Indicator source must be specified'

    # Instantiate the StorageManager
    async with StorageManager() as storage_manager:
        try:
            # Compose the path to the source config file
            source_config_path = f'{storage_manager.SOURCES_CFG_PATH}/{indicator_source.lower()}/{indicator_source.lower()}.cfg'

            # Check if the blob exists in Azure Storage
            source_cfg_file_exists = await storage_manager.check_blob_exists(blob_name=source_config_path)
            if not source_cfg_file_exists:
                raise Exception(f'Source config file {source_config_path} not found for {indicator_source}')

            # Get the source configuration from the config file
            source_cfg = await storage_manager.get_source_cfg(source_path=source_config_path)

            # Compose the path to the source file
            source_file_name = os.path.join(storage_manager.SOURCES_PATH,
                                            f"{indicator_source.upper()}.{source_cfg['source']['file_format']}")

            # Download the source file from Azure Storage
            data = await storage_manager.cached_download(source_path=source_file_name)

            # Return the data along with the source configuration
            return data, source_cfg

        except Exception as e:
            # Log the error raise the exception
            logger.error(e)
            raise


async def run_transformation_for_indicator(indicator_cfg: dict = None, project: str = None):
    """
    Run transformation for a specific indicator.

    :param indicator_cfg: Configuration section for the indicator.
    :type indicator_cfg: dict
    :param project: The project to run the transformation for
    :return: The transformed data based on the indicator.
    :rtype: str
    """

    # Ensure the indicator configuration is provided
    assert indicator_cfg is not None, 'Indicator config must be specified'
    try:
        # Get the indicator ID from the configuration
        indicator_id = indicator_cfg['indicator_id']

        # Read the source file for the indicator asynchronously
        source_data_bytes, source_cfg = await read_source_file_for_indicator(
            indicator_source=indicator_cfg['source_id'])

        # Get the preprocessing function for the indicator from the 'preprocessing' field in the configuration
        preprocessing_function_name = indicator_cfg['preprocessing']
        assert hasattr(preprocessing,
                       preprocessing_function_name), f'The preprocessing function {preprocessing_function_name} specified in ' \
                                                     f'indicator config "{indicator_id}" was not implemented'
        preprocessing_function = getattr(preprocessing, preprocessing_function_name)

        # Retrieve necessary columns for transformation from the source configuration
        country_column = r"{}".format(source_cfg['source'].get('country_name_column', None))
        group_name = indicator_cfg.get('group_name')
        key_column = source_cfg['source'].get('country_iso3_column', None)
        sheet_name = indicator_cfg.get('sheet_name', None)
        year = source_cfg['source'].get('year', None)

        # Preprocess the source data using the preprocessing function
        source_df = await preprocessing_function(
            bytes_data=source_data_bytes,
            sheet_name=sheet_name,
            year=year,
            group_name=group_name,
            country_column=country_column,
            key_column=key_column,
            indicator_id=indicator_id
        )

        # Replace '..' with NaN and drop columns with all NaN values
        source_df.replace('..', np.NaN, inplace=True)
        source_df.dropna(inplace=True, axis=1, how="all")
        # Get the transform function for the indicator from the 'transform_function' field in the configuration
        transform_function_name = indicator_cfg.get('transform_function', None)

        if transform_function_name is not None:
            # If a transform function is specified, run it

            # Get the transform function from the 'transform_functions' module
            run_transform = getattr(transform_functions, transform_function_name)

            # Modify column names if needed based on the source information
            source_info = source_cfg['source']
            country_column = source_info.get('country_name_column', None)
            key_column = source_info.get('country_iso3_column', None)

            if country_column == "None":
                country_column = None
            else:
                country_column = source_info.get('country_name_column', None)
                source_df.rename(columns={country_column: 'Country'}, inplace=True)
                country_column = 'Country'

            if key_column == "None":
                key_column = None
            else:
                key_column = source_info.get('country_iso3_column', key_column)
                source_df.rename(columns={key_column: 'Alpha-3 code'}, inplace=True)
                key_column = 'Alpha-3 code'

            # Log the transform function being executed
            logger.info(f"Running transform function {transform_function_name} for indicator {indicator_id}")
            # Execute the transform function with specified parameters
            await run_transform(
                source_df=source_df,
                indicator_id=indicator_id,
                value_column=indicator_cfg.get('value_column', None),
                base_filename=None if source_info.get('id', None) == "None" else source_info.get('id', None),
                country_column=country_column,
                key_column=key_column,
                datetime_column=None if source_info.get('datetime_column', None) == "None" else source_info.get(
                    'datetime_column', None),
                group_column=None if source_info.get('group_column', None) == "None" else source_info.get(
                    'group_column', None),
                group_name=None if source_info.get('group_name', None) == "None" else source_info.get('group_name',
                                                                                                      None),
                aggregate=False if source_info.get('aggregate', None) != "True" else True,
                aggregate_type="sum",
                keep="last",
                country_code_aggregate=False if source_info.get('country_code_aggregate', None) != "True" else True,
                return_dataframe=False,
                region_column=None if source_info.get('region_column', None) == "None" else source_info.get(
                    'region_column', None),
                year=indicator_cfg.get('year', None),
                column_prefix=None if indicator_cfg.get('column_prefix', None) == "None" else indicator_cfg.get(
                    'column_prefix', None),
                column_suffix=None if indicator_cfg.get('column_suffix', None) == "None" else indicator_cfg.get(
                    'column_suffix', None),
                column_substring=None if indicator_cfg.get('column_substring', None) == "None" else indicator_cfg.get(
                    'column_substring', None),
                project=project,
                #     The following arguments are used only in the sme_transform function
                dividend=None if indicator_cfg.get('dividend', None) == "None" else indicator_cfg.get('dividend', None),
                divisor=None if indicator_cfg.get('divisor', None) == "None" else indicator_cfg.get('divisor', None),
            )

        else:
            # If no transform function is specified, log the information
            logger.info(f"No transform function specified for indicator {indicator_id}")

        return indicator_id
    except Exception as e:
        raise e


async def transform_sources(concurrent=False,
                            indicator_ids: List = None,
                            project: str = None,
                            concurrent_chunk_size: int = 50) -> List[str]:
    """
    Perform transformations for a list of indicators.
    """
    failed_indicators_ids = list()
    skipped_indicators_id = list()

    # logger.info(f'Tranforming {len(indicator_ids)}')
    # Initialize the StorageManager
    async with StorageManager() as storage_manager:

        indicators_cfgs = await storage_manager.get_indicators_cfg(indicator_ids=indicator_ids)

        for chunk in chunker(indicators_cfgs, concurrent_chunk_size):

            # await storage_manager.delete_blob(blob_path=os.path.join('DataFuturePlatform', 'pipeline', 'config', 'indicators', 'mmrlatest_gii.cfg'))
            tasks = list()
            # List to store transformed indicator IDs
            transformed_indicators = list()
            # Loop through each indicator configuration and perform transformations
            for indicator_cfg in chunk:
                indicator_section = indicator_cfg['indicator']
                indicator_id = indicator_section['indicator_id']
                if indicator_section.get('preprocessing') is None:
                    # Skip if no preprocessing function is specified
                    logger.info(
                        f"Skipping preprocessing for indicator {indicator_id} as no preprocessing function is specified")
                    skipped_indicators_id.append(indicator_id)
                    continue

                if not concurrent:
                    # Perform transformation sequentially
                    transformed_indicator_id = await run_transformation_for_indicator(indicator_cfg=indicator_section,
                                                                                      project=project)
                    transformed_indicators.append(transformed_indicator_id)
                else:
                    # Create a task for running the transformation for the indicator
                    # Perform transformation concurrently using asyncio tasks
                    transformation_task = asyncio.create_task(
                        run_transformation_for_indicator(indicator_cfg=indicator_section, project=project),
                        name=indicator_id
                    )
                    tasks.append(transformation_task)

            if concurrent:
                done, pending = await asyncio.wait(tasks, timeout=3600 * 3, return_when=asyncio.ALL_COMPLETED)

                for task in done:
                    indicator_id = task.get_name()
                    try:
                        await task
                        logger.info(f'Transform for {indicator_id} was executed successfully')
                        transformed_indicators.append(indicator_id)
                    except Exception as e:
                        failed_indicators_ids.append(indicator_id)
                        with StringIO() as m:
                            print_exc(file=m)
                            em = m.getvalue()
                            logger.error(f'Error {em} was encountered while processing  {indicator_id}')

                    # if task.exception():
                    #     logger.error(f'Transform for {indicator_id} failed with error:\n'
                    #                  f'"{task.exception()}"')
                    #     await asyncio.sleep(3)
                    # else:
                    #     logger.info(f'Transform for {indicator_id} was executed successfully')
                    #     transformed_indicators.append(indicator_id)
                # # Handle timed out tasks
                for task in pending:
                    # Cancel task and wait for cancellation to complete
                    indicator_id = task.get_name()
                    failed_indicators_ids.append(indicator_id)
                    task.cancel()
                    await task

        logger.info(
            f'Transformation complete for {len(transformed_indicators)} indicators with {len(failed_indicators_ids)} failed indicators')
        return transformed_indicators


if __name__ == "__main__":
    logging.basicConfig()
    logger = logging.getLogger("azure.storage.blob")
    logging_stream_handler = logging.StreamHandler()
    logging_stream_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(logging_stream_handler)
    logger.name = __name__
    asyncio.run(transform_sources())
