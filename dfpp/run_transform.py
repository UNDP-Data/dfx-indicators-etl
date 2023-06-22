import json
import os
import asyncio
import logging
import numpy as np
from dfpp.utils import chunker
from configparser import ConfigParser, RawConfigParser
from dfpp import preprocessing
from dfpp.storage import AsyncAzureBlobStorageManager
from dfpp.constants import  SOURCE_CONFIG_ROOT_FOLDER, INDICATOR_CONFIG_ROOT_FOLDER

# This is importing all transform functions from transform_functions.py. DO NOT REMOVE EVEN IF IDE SAYS IT IS UNUSED
from dfpp import transform_functions
logger = logging.getLogger(__name__)

# indicator_parser = ConfigParser(interpolation=None)
CONNECTION_STRING=os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
CONTAINER_NAME=os.environ.get('AZURE_STORAGE_CONTAINER_NAME')
ROOT_FOLDER = os.environ.get('ROOT_FOLDER')


def cfg2dict(config_object=None):
    """
    Copnverts a config object to dict
    :param config_object:
    :return:
    """
    output_dict=dict()
    sections=config_object.sections()
    for section in sections:
        items=config_object.items(section)
        output_dict[section]=dict(items)
    return output_dict



class UnescapedConfigParser(RawConfigParser):
    """
    An extension of the RawConfigParser that does not escape values when reading from a file.
    """
    def get(self, section, option, **kwargs):
        """
        Get the value of an option.
        :param section: The section of the config file.
        :param option: The option to get the value of.
        :param kwargs: The keyword arguments.
        :return:
        """
        value = super().get(section, option, **kwargs)
        try:
            return value.encode().decode('unicode_escape')
        except AttributeError:
            return value




async def read_indicator(storage_manager=None, blob_name=None):
    logger.debug(f'Downloading config for indicator {blob_name}')
    file_in_bytes = await storage_manager.download(blob_name=blob_name)
    indicator_parser = ConfigParser(interpolation=None)
    indicator_parser.read_string(file_in_bytes.decode('utf-8'))
    if indicator_parser['indicator']['indicator_id'] == 'CPIA_CIR':
        print(blob_name)
    return {
        "id": indicator_parser['indicator']['indicator_id'],
        "source": indicator_parser['indicator']['source_id'],
        "preprocessing": indicator_parser['indicator']['preprocessing'],
        "sheet_name": indicator_parser['indicator'].get('sheet_name', None),
        "transform_function": indicator_parser['indicator'].get('transform_function', None),
        "indicator_name": indicator_parser['indicator'].get('indicator_name', None),
        "value_column": indicator_parser['indicator'].get('value_column', None),
        "column_substring": indicator_parser['indicator'].get('column_substring', None),
        "column_prefix": indicator_parser['indicator'].get('column_prefix', None),
        "column_suffix": indicator_parser['indicator'].get('column_suffix', None),
        "year": indicator_parser['indicator'].get('year', None),
        "group_name": indicator_parser['indicator'].get('year', None)
    }



async def read_indicators_config():
    storage_manager = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=CONNECTION_STRING,
        container_name=CONTAINER_NAME,
        use_singleton=False
    )
    indicator_list = []
    futures = []
    for blob in await storage_manager.list_blobs(prefix=INDICATOR_CONFIG_ROOT_FOLDER):
        file = blob.name
        if file.endswith("_cpiacir.cfg"):

            futures.append(asyncio.ensure_future(
                read_indicator(storage_manager=storage_manager,blob_name=file)
            ))

    for chunk in chunker(futures, 50):
        logging.info(f'Fetching configs for {len(chunk)} indicators')
        for fut in asyncio.as_completed(chunk):
            indicator_list.append(await fut)

    await storage_manager.close()
    return sorted(indicator_list,key=lambda d:d['id'])


async def read_source_file_for_indicator(indicator_source: str = None):
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=CONNECTION_STRING,
        container_name=CONTAINER_NAME,
        use_singleton=False
    )
    try:

        source_config_path = f'{SOURCE_CONFIG_ROOT_FOLDER}/{indicator_source.lower()}/{indicator_source.lower()}.cfg'
        print(source_config_path)
        # check the blob exists in azure storage
        source_file_exists = await storage.check_blob_exists(blob_name=source_config_path)
        if not source_file_exists:
            logger.error(f"Source config file not found for {indicator_source}")
            return None

        blob_as_bytes = await storage.download(blob_name=source_config_path, dst_path=None)
        source_parser = UnescapedConfigParser(
            interpolation=None
        )

        source_parser.read_string(blob_as_bytes.decode('utf-8'))
        source_cfg = cfg2dict(source_parser)


        source_file_path = os.path.join(ROOT_FOLDER, 'sources', 'raw',
                                        f"{indicator_source.upper()}.{source_parser['source']['file_format']}")
        data = await storage.download(blob_name=source_file_path, dst_path=None)
        await storage.close()
        return data, source_cfg
    except Exception as e:
        logger.error(e)
        await storage.close()
        return None


async def run_transformation_for_indicator(indicator_cfg:dict=None):
    """
    Run transformation for a specific indicator.

    Args:
        indicator_cfg (str): Configuration for the indicator.

    Returns:
        The transformed data based on the indicator.
    """

    indicator_id = indicator_cfg['id']

    # Read source file for the indicator asynchronously
    source_data_bytes, source_cfg = await read_source_file_for_indicator(indicator_source=indicator_cfg['source'])

    # # Read the indicator list asynchronously
    # indicator_list = await read_indicators_config()

    if source_data_bytes is None:
        return None



    # Get the preprocessing function for the indicator
    preprocessing_function_name = indicator_cfg['preprocessing']
    assert hasattr(preprocessing, preprocessing_function_name), f'Indicator {indicator_id} does not define a preprocessing fucntion'
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
        return await run_transform(
            source_df=source_df,
            indicator_id=indicator_id,
            value_column=indicator_cfg.get('value_column', None),
            base_filename=None if source_info.get('id', None) == "None" else source_info.get('id', None),
            country_column=country_column,
            key_column=key_column,
            datetime_column=None if source_info.get('datetime_column', None) == "None" else source_info.get('datetime_column', None),
            group_column=None if source_info.get('group_column', None) == "None" else source_info.get('group_column',None),
            group_name=None if source_info.get('group_name', None) == "None" else source_info.get('group_name', None),
            aggregate=False,
            aggregate_type="sum",
            keep="last",
            country_code_aggregate=False,
            return_dataframe=False,
            region_column=None if source_info.get('region_column', None) == "None" else source_info.get('region_column', None),
            year=indicator_cfg.get('year', None),
            column_prefix=None if indicator_cfg.get('column_prefix', None) == "None" else indicator_cfg.get('column_prefix', None),
            column_suffix=None if indicator_cfg.get('column_suffix', None) == "None" else indicator_cfg.get('column_suffix', None),
            column_substring=None if indicator_cfg.get('column_substring', None) == "None" else indicator_cfg.get('column_substring', None),
        )
    else:
        logger.info(f"No transform function specified for indicator {indicator_id}")


async def transform_sources():
    """
    Perform transformations for a list of indicators.
    """

    indicator_list = await read_indicators_config()
    tasks = []

    for indicator in indicator_list:
        # if indicator.get("id") == "populationlivinginslums_cpiaplis":
        if indicator.get('preprocessing') is None:
            # Skip if no preprocessing function is specified
            logger.info(
                f"Skipping preprocessing for indicator {indicator['id']} as no preprocessing function is specified")
            continue

        # Create a task for running the transformation for the indicator
        transformation_task = asyncio.create_task(run_transformation_for_indicator(indicator_cfg=indicator))
        tasks.append(transformation_task)
        print(json.dumps(indicator, indent=4))


    # Gather all the transformation tasks
    # transformation_tasks = await asyncio.gather(*tasks)
    transformation_tasks = await asyncio.gather(*tasks, return_exceptions=False)

    for task in transformation_tasks:
        if isinstance(task, Exception) or task is None:
            logger.error("Task failed:")
        else:
            logger.info("Task Successful:")


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
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(logging_stream_handler)
    logger.name = __name__
    asyncio.run(transform_sources())
