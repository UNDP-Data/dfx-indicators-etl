import json
import os
import asyncio
import logging
from typing import List
import numpy as np
from dfpp.storage import StorageManager
from dfpp import preprocessing

# This is importing all transform functions from transform_functions.py. DO NOT REMOVE EVEN IF IDE SAYS IT IS UNUSED
from dfpp import transform_functions

logger = logging.getLogger(__name__)




async def read_source_file_for_indicator(indicator_source: str = None):
    async with StorageManager() as storage_manager:
        try:

            source_config_path = f'{storage_manager.SOURCES_CFG_PATH}/{indicator_source.lower()}/{indicator_source.lower()}.cfg'

            # check the blob exists in azure storage
            source_cfg_file_exists = await storage_manager.check_blob_exists(blob_name=source_config_path)
            if not source_cfg_file_exists:
                raise Exception(f'Source config file {source_config_path} not found for {indicator_source}')
            source_cfg = await storage_manager.get_source_cfg(source_path=source_config_path)

            source_file_name = os.path.join(storage_manager.SOURCES_PATH,f"{indicator_source.upper()}.{source_cfg['source']['file_format']}")
            data = await storage_manager.cached_download(source_path=source_file_name)
            await storage_manager.close()
            return data, source_cfg
        except Exception as e:
            logger.error(e)
            await storage_manager.close()
            raise


async def run_transformation_for_indicator(indicator_cfg: dict = None):
    """
    Run transformation for a specific indicator.

    Args:
        indicator_cfg (str): Configuration section for the indicator.

    Returns:
        The transformed data based on the indicator.
    """

    indicator_id = indicator_cfg['indicator_id']

    # Read source file for the indicator asynchronously
    source_data_bytes, source_cfg = await read_source_file_for_indicator(indicator_source=indicator_cfg['source_id'])

    # Get the preprocessing function for the indicator
    preprocessing_function_name = indicator_cfg['preprocessing']
    assert hasattr(preprocessing,
                   preprocessing_function_name), f'The preprocessing function {preprocessing_function_name} specified in ' \
                                                 f'indicator config "{indicator_id}" was not implemented'
    preprocessing_function = getattr(preprocessing, preprocessing_function_name)

    # Retrieve necessary columns for transformation
    country_column = r"{}".format(source_cfg['source'].get('country_name_column', None))
    group_name = indicator_cfg.get('group_name')
    key_column = source_cfg.get('country_iso3_column', None)
    sheet_name = indicator_cfg.get('sheet_name', None)
    year = source_cfg.get('year', None)

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

    source_df.replace('..', np.NaN, inplace=True)
    source_df.dropna(inplace=True, axis=1, how="all")
    transform_function_name = indicator_cfg.get('transform_function', None)

    if transform_function_name is not None:
        # If transform function is specified, run it

        run_transform = getattr(transform_functions, transform_function_name)
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


        logger.info(f"Running transform function {transform_function_name} for indicator {indicator_id}")

        await run_transform(
            source_df=source_df,
            indicator_id=indicator_id,
            value_column=indicator_cfg.get('value_column', None),
            base_filename=None if source_info.get('id', None) == "None" else source_info.get('id', None),
            country_column=country_column,
            key_column=key_column,
            datetime_column=None if source_info.get('datetime_column', None) == "None" else source_info.get(
                'datetime_column', None),
            group_column=None if source_info.get('group_column', None) == "None" else source_info.get('group_column',
                                                                                                      None),
            group_name=None if source_info.get('group_name', None) == "None" else source_info.get('group_name', None),
            aggregate=False,
            aggregate_type="sum",
            keep="last",
            country_code_aggregate=False,
            return_dataframe=False,
            region_column=None if source_info.get('region_column', None) == "None" else source_info.get('region_column',
                                                                                                        None),
            year=indicator_cfg.get('year', None),
            column_prefix=None if indicator_cfg.get('column_prefix', None) == "None" else indicator_cfg.get(
                'column_prefix', None),
            column_suffix=None if indicator_cfg.get('column_suffix', None) == "None" else indicator_cfg.get(
                'column_suffix', None),
            column_substring=None if indicator_cfg.get('column_substring', None) == "None" else indicator_cfg.get(
                'column_substring', None),
        )

    else:
        logger.info(f"No transform function specified for indicator {indicator_id}")
    return indicator_id


async def transform_sources(concurrent=False, indicator_ids: List = None):
    """
    Perform transformations for a list of indicators.
    """


    async with StorageManager() as storage_manager:
        if indicator_ids is not None:
            indicators_cfgs = await storage_manager.get_indicators_cfg(indicator_ids=indicator_ids)
        else:
            indicators_cfgs = await storage_manager.get_indicators_cfg()

        # await storage_manager.delete_blob(blob_path=os.path.join('DataFuturePlatform', 'pipeline', 'config', 'indicators', 'mmrlatest_gii.cfg'))
        tasks = list()
        transformed_indicators = list()
        for indicator_cfg in indicators_cfgs:
            indicator_section = indicator_cfg['indicator']
            indicator_id = indicator_section['indicator_id']
            if indicator_section.get('preprocessing') is None:
                # Skip if no preprocessing function is specified
                logger.info(
                    f"Skipping preprocessing for indicator {indicator_id} as no preprocessing function is specified")
                continue

            if not concurrent:
                transformed_indicator_id = await run_transformation_for_indicator(indicator_cfg=indicator_section)
                transformed_indicators.append(transformed_indicator_id)
            else:
                # Create a task for running the transformation for the indicator
                transformation_task = asyncio.create_task(
                    run_transformation_for_indicator(indicator_cfg=indicator_section),
                    name=indicator_id
                )
                tasks.append(transformation_task)

        if concurrent:
            done, pending = await asyncio.wait(tasks, timeout=3600 * 3, return_when=asyncio.ALL_COMPLETED)

            for task in done:
                indicator_id = task.get_name()
                if task.exception():
                    logger.error(f'Transform for {indicator_id} failed with error:\n'
                                 f'"{task.exception()}"')
                    await asyncio.sleep(3)
                else:
                    logger.info(f'Transform for {indicator_id} was executed successfully')
                    transformed_indicators.append(indicator_id)
            # handle timed out
            for task in pending:
                # cancel task
                task.cancel()
                await task
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
