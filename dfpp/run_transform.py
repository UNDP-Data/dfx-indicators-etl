from configparser import ConfigParser, RawConfigParser
from preprocessing import *
from constants import *

# This is importing all transform functions from transform_functions.py. DO NOT REMOVE EVEN IF IDE SAYS IT IS UNUSED
from transform_functions import *


indicator_parser = ConfigParser(interpolation=None)


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


source_parser = UnescapedConfigParser(
    interpolation=None
)


async def read_indicator_list():
    indicator_config_path = 'migration/indicator_cfg_files/'
    indicator_list = []
    for file in os.listdir(indicator_config_path):
        if file.endswith(".cfg"):
            indicator_parser.read(indicator_config_path + file)
            indicator_list.append({
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
            })
    return indicator_list


async def read_source_file_for_indicator(indicator_id: str = None):
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=AZURE_STORAGE_CONNECTION_STRING,
        container_name=AZURE_STORAGE_CONTAINER_NAME,
        use_singleton=False
    )
    try:
        # construct the path to the source file by reading the indicator config file
        indicator_config_path = 'migration/indicator_cfg_files/{indicator}.cfg'.format(indicator=indicator_id)
        indicator_parser.read(indicator_config_path)
        source = indicator_parser['indicator']['source_id']

        source_config_path = f'migration/source_cfg_files/{source.lower()}/{source.lower()}.cfg'
        if not os.path.exists(source_config_path):
            logger.error(f"Source config file not found for {source}")
            return None
        source_parser.read(source_config_path)
        source_file_path = os.path.join(ROOT_FOLDER, 'sources', 'raw',
                                        f"{source.upper()}.{source_parser['source']['file_format']}")
        data = await storage.download(blob_name=source_file_path, dst_path=None)
        await storage.close()
        return data
    except Exception as e:
        logger.error(e)
        await storage.close()
        return None


async def run_transformation_for_indicator(**kwargs):
    """
    Run transformation for a specific indicator.

    Args:
        indicator_id (str): Identifier for the indicator.

    Returns:
        The transformed data based on the indicator.
    """

    indicator_id = kwargs.get('indicator_id', None)

    # Read source file for the indicator asynchronously
    source_in_bytes = await read_source_file_for_indicator(indicator_id=indicator_id)

    # Read the indicator list asynchronously
    indicator_list = await read_indicator_list()

    if source_in_bytes is None:
        return None

    # Retrieve the indicator based on its id from the indicator list
    indicator = [indicator for indicator in indicator_list if indicator['id'] == indicator_id][0]

    # Get the preprocessing function for the indicator
    preprocessing_function = globals()[indicator['preprocessing']]

    # Read the source parser configuration file
    source_id = indicator.get('source', None)
    source_parser.read(f'migration/source_cfg_files/{source_id.lower()}/{source_id.lower()}.cfg')

    # Retrieve necessary columns for transformation
    country_column = r"{}".format(source_parser['source'].get('country_name_column', None))
    group_name = indicator_parser['indicator'].get('group_name', None)
    key_column = source_parser['source'].get('country_iso3_column', None)
    sheet_name = indicator.get('sheet_name', None)
    year = source_parser['source'].get('year', None)

    # Preprocess the source data using the preprocessing function
    source_df = await preprocessing_function(
        bytes_data=source_in_bytes,
        sheet_name=sheet_name,
        year=year,
        country_column=country_column,
        key_column=key_column,
        indicator_id=indicator_id,
        indicator=indicator
    )

    source_df.replace('..', np.NaN, inplace=True)
    source_df.dropna(inplace=True, axis=1, how="all")

    transform_function = indicator.get('transform_function', None)

    if transform_function is not None:
        # If transform function is specified, run it
        run_transform = globals()[transform_function]
        source_info = source_parser['source']
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
        logger.info(f"Running transform function {transform_function} for indicator {indicator_id}")
        return await run_transform(
            source_df=source_df,
            indicator_id=indicator_id,
            value_column=indicator.get('value_column', None),
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
            year=indicator.get('year', None),
            column_prefix=None if indicator.get('column_prefix', None) == "None" else indicator.get('column_prefix', None),
            column_suffix=None if indicator.get('column_suffix', None) == "None" else indicator.get('column_suffix', None),
            column_substring=None if indicator.get('column_substring', None) == "None" else indicator.get('column_substring', None),
        )
    else:
        logger.info(f"No transform function specified for indicator {indicator_id}")


async def transform_sources():
    """
    Perform transformations for a list of indicators.
    """

    indicator_list = await read_indicator_list()
    tasks = []

    for indicator in indicator_list:
        # if indicator.get("id") == "populationlivinginslums_cpiaplis":
        if indicator.get('preprocessing') is None:
            # Skip if no preprocessing function is specified
            logger.info(
                f"Skipping preprocessing for indicator {indicator['id']} as no preprocessing function is specified")
            continue

        # Create a task for running the transformation for the indicator
        transformation_task = asyncio.create_task(run_transformation_for_indicator(indicator_id=indicator['id']))
        tasks.append(transformation_task)

    # Gather all the transformation tasks
    # transformation_tasks = await asyncio.gather(*tasks)
    transformation_tasks = await asyncio.gather(*tasks, return_exceptions=True)

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
