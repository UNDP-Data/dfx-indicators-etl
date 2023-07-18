import json
import os
import asyncio
import logging
import tempfile

import numpy as np

from dfpp.storage import StorageManager
from dfpp.utils import chunker
from configparser import ConfigParser, RawConfigParser
from dfpp import preprocessing
from dfpp.constants import SOURCE_CONFIG_ROOT_FOLDER, INDICATOR_CONFIG_ROOT_FOLDER
import ast
# This is importing all transform functions from transform_functions.py. DO NOT REMOVE EVEN IF IDE SAYS IT IS UNUSED
from dfpp import transform_functions

logger = logging.getLogger(__name__)

# indicator_parser = ConfigParser(interpolation=None)
CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
CONTAINER_NAME = os.environ.get('AZURE_STORAGE_CONTAINER_NAME')
ROOT_FOLDER = os.environ.get('ROOT_FOLDER')
TMP_SOURCES = {}

MANDATORY_SOURCE_COLUMNS = 'id', 'url', 'save_as'


def validate_src_cfg(cfg_dict=None, cfg_file_path=None):
    """
    Validate a source config file
    :param cfg_dict:
    :param cfg_file_path:
    :return:
    """
    assert cfg_dict is not None, f'Invalid source config {cfg_dict}'
    assert cfg_dict is not {}, f'Invalid source config {cfg_dict}'

    for k in MANDATORY_SOURCE_COLUMNS:
        v = cfg_dict['source'][k]
        try:
            v_parsed = ast.literal_eval(v)
            assert v_parsed is not None, f"{k} key {cfg_file_path} needs to be a valid string. Current value is {v}"
        except AssertionError:
            raise
        except Exception as e:
            pass

        assert k in cfg_dict['source'], f'{cfg_file_path} needs to contain {k} key'
        assert isinstance(v, str), f"{cfg_file_path}'s {k} key needs to be a string. Current value is {type(v)}"
        assert v, f"{cfg_file_path}'s {k} key needs to be a valid string. Current value is {v}"


def cfg2dict(config_object=None):
    """
    Copnverts a config object to dict
    :param config_object:
    :return:
    """
    output_dict = dict()
    sections = config_object.sections()
    for section in sections:
        items = config_object.items(section)
        output_dict[section] = dict(items)
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


async def read_indicator(storage_manager=None, blob_rel_path=None):
    """
    Read the configuration file of a DFP indicator stored in blob_rel_path
    :param storage_manager:
    :param blob_rel_path:
    :return: the configuration file as a dict
    """

    logger.info(f'Downloading config {blob_rel_path.split("/")[-1]}')
    file_in_bytes = await storage_manager.download(blob_name=blob_rel_path)

    indicator_parser = ConfigParser(interpolation=None)
    indicator_parser.read_string(file_in_bytes.decode('utf-8'))

    cfg_dict = cfg2dict(indicator_parser)

    assert 'indicator' in cfg_dict, f'Indicator config file  {blob_rel_path} is invalid'
    assert cfg_dict, f'Indicator config file  {blob_rel_path} is invalid'

    return cfg_dict


# async def read_indicators_config(indicator_file_contains=None, chunk_size=50):
#     """
#     Read indicators config from azure blob container
#     :param indicator_file_contains: str, if supplied used to filter the indicators
#     :param chunk_size: int, default=50, split the indicators  in chunks with size = chunk_size and
#            read them concurrently
#     :return:
#     """
#     async with StorageManager(connection_string=CONNECTION_STRING, container_name=CONTAINER_NAME) as storage_manager:
#         indicator_list = []
#         tasks = []
#
#         # for blob in await storage_manager.list_blobs(prefix=INDICATOR_CONFIG_ROOT_FOLDER):
#         #     indicator_rel_path = blob.name
#         #     if not indicator_rel_path.endswith('.cfg'):
#         #         continue
#         #     if indicator_file_contains and indicator_file_contains not in indicator_rel_path:
#         #         continue
#         #
#         #     tasks.append(asyncio.create_task(
#         #         read_indicator(storage_manager=storage_manager, blob_rel_path=indicator_rel_path),
#         #         name=indicator_rel_path
#         #     ))
#
#         for chunk in chunker(tasks, 50):
#             logging.info(f'Fetching configs for {len(chunk)} indicators')
#             done, pending = await asyncio.wait(chunk, timeout=None, return_when=asyncio.ALL_COMPLETED)
#             for t in done:
#                 if t.exception():
#                     logger.error(f'Failed to read indicator config file {t.get_name()} because {t.exception()}')
#                 else:
#                     indicator_list.append(await t)
#
#         return sorted(indicator_list, key=lambda d: d['indicator']['indicator_id'])


async def read_source_file_for_indicator(indicator_source: str = None):
    async with StorageManager(connection_string=CONNECTION_STRING, container_name=CONTAINER_NAME) as storage_manager:
        try:

            source_config_path = f'{SOURCE_CONFIG_ROOT_FOLDER}/{indicator_source.lower()}/{indicator_source.lower()}.cfg'

            # check the blob exists in azure storage
            source_cfg_file_exists = await storage_manager.check_blob_exists(blob_name=source_config_path)
            if not source_cfg_file_exists:
                raise Exception(f'Source config file {source_config_path} not found for {indicator_source}')

            blob_as_bytes = await storage_manager.download(blob_name=source_config_path, dst_path=None)
            source_parser = UnescapedConfigParser(
                interpolation=None
            )

            source_parser.read_string(blob_as_bytes.decode('utf-8'))
            source_cfg = cfg2dict(source_parser)

            validate_src_cfg(cfg_dict=source_cfg, cfg_file_path=source_config_path)

            source_file_name = f"{indicator_source.upper()}.{source_parser['source']['file_format']}"
            if source_file_name in TMP_SOURCES:
                cached_src_path = TMP_SOURCES[source_file_name]
                logger.info(f'Rereading {source_file_name} from {cached_src_path} ')
                data = open(cached_src_path, 'rb').read()
            else:
                source_file_path = os.path.join(ROOT_FOLDER, 'sources', 'raw', source_file_name)
                # check the blob exists in azure storage
                source_file_exists = await storage_manager.check_blob_exists(blob_name=source_file_path)
                if not source_file_exists:
                    raise Exception(f'Source file {source_file_path} not found for {indicator_source}')
                data = await storage_manager.download(blob_name=source_file_path, dst_path=None)
                cached = tempfile.NamedTemporaryFile(mode='wb', delete=False)
                logger.debug(f'Going to cache {source_file_name} to {cached.name}')
                cached.write(data)
                TMP_SOURCES[source_file_name] = cached.name
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


async def transform_sources(concurrent=False):
    """
    Perform transformations for a list of indicators.
    """

    # indicator_list = await read_indicators_config(indicator_file_contains="_cpiacir.cfg")

    # indicator_list = await read_indicators_config(indicator_file_contains='vdem')
    async with StorageManager(connection_string=CONNECTION_STRING, container_name=CONTAINER_NAME) as storage_manager:
        indicators_cfgs = await storage_manager.get_indicators_cfg()
        # await storage_manager.delete_blob(blob_path=os.path.join('DataFuturePlatform', 'pipeline', 'config', 'indicators', 'mmrlatest_gii.cfg'))
        tasks = list()
        for indicator_cfg in indicators_cfgs:
            print(indicator_cfg)
            indicator_section = indicator_cfg['indicator']
            indicator_id = indicator_section['indicator_id']
            if indicator_section.get('preprocessing') is None:
                # Skip if no preprocessing function is specified
                logger.info(
                    f"Skipping preprocessing for indicator {indicator_id} as no preprocessing function is specified")
                continue

            if not concurrent:
                await run_transformation_for_indicator(indicator_cfg=indicator_section)
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
            # handle timed out
            for task in pending:
                # cancel task
                task.cancel()
                await task

        for k, v in TMP_SOURCES.items():
            exists = os.path.exists(v)
            if exists:
                logger.info(f'Removing cache {v} for source {k} ')
                os.remove(v)


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
