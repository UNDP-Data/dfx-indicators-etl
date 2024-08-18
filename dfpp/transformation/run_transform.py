import asyncio
import logging
import os

import numpy as np

from dfpp.storage import StorageManager
from dfpp.transformation import preprocessing, transform_functions

logger = logging.getLogger(__name__)


def log_exceptions(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            raise

    return wrapper


@log_exceptions
async def read_source_file_for_indicator(indicator_source: str = None):
    """
    Read the source file for a specific indicator asynchronously.
    """
    async with StorageManager() as storage_manager:
        source_config_path = os.path.join(
            storage_manager.sources_cfg_path,
            indicator_source.lower(),
            f"{indicator_source.lower()}.cfg",
        )

        source_cfg_file_exists = await storage_manager.check_blob_exists(
            blob_name=source_config_path
        )
        if not source_cfg_file_exists:
            raise FileNotFoundError(
                f"Source config file {source_config_path} not found for {indicator_source}"
            )

        source_cfg = await storage_manager.get_source_cfg(
            source_id_or_path=source_config_path
        )

        source_file_name = os.path.join(
            storage_manager.sources_path,
            f"{indicator_source.upper()}.{source_cfg['file_format']}",
        )

        data = await storage_manager.read_blob(path=source_file_name)

        logger.debug(f"Downloaded {source_file_name}")
        return data, source_cfg


@log_exceptions
async def run_transformation_for_indicator(indicator_cfg: dict = None):
    """
    Run transformation for a specific indicator.

    :param indicator_cfg: Configuration section for the indicator.
    :type indicator_cfg: dict
    :return: The transformed data based on the indicator.
    :rtype: str
    """
    indicator_id = indicator_cfg["indicator_id"]
    source_data_bytes, source_cfg = await read_source_file_for_indicator(
        indicator_source=indicator_cfg["source_id"]
    )
    preprocessing_function_name = indicator_cfg["preprocessing"]
    preprocessing_function = getattr(preprocessing, preprocessing_function_name)

    # Retrieve necessary columns for transformation from the source configuration
    country_column = source_cfg["country_name_column"]
    key_column = source_cfg["country_iso3_column"]
    datetime_column = source_cfg["datetime_column"]
    year = source_cfg["year"]

    group_name = indicator_cfg["group_name"]
    sheet_name = indicator_cfg["sheet_name"]

    source_df = await preprocessing_function(
        bytes_data=source_data_bytes,
        sheet_name=sheet_name,
        year=year,
        group_name=group_name,
        country_column=country_column,
        datetime_column=datetime_column,
        key_column=key_column,
        indicator_id=indicator_id,
    )

    # Replace '..' with NaN and drop columns with all NaN values
    source_df.replace("..", np.NaN, inplace=True)
    source_df.dropna(inplace=True, axis=1, how="all")

    run_transform = getattr(transform_functions, indicator_cfg["transform_function"])

    # Modify column names if needed based on the source information
    rename_columns = {country_column: "Country", key_column: "Alpha-3 code"}
    source_df.rename(columns=rename_columns, inplace=True)
    logger.info(
        f"Running transform function {indicator_cfg['transform_function']} for indicator {indicator_id}"
    )

    # TBD: Run transformation and return dataframe, upload then
    await run_transform(
        source_df=source_df,
        indicator_id=indicator_id,
        value_column=indicator_cfg["value_column"],
        base_filename=source_cfg["id"],
        country_column="Country",
        key_column="Alpha-3 code",
        datetime_column=datetime_column,
    )

    return indicator_id


async def process_indicator(
    indicator_cfg: dict,
    semaphore: asyncio.Semaphore,
    processing_statuses: dict[str, list],
):
    """
    Process a single indicator, applying transformations and updating processing statuses.
    """
    indicator_id = indicator_cfg["indicator_id"]
    async with semaphore:
        try:
            await asyncio.wait_for(
                run_transformation_for_indicator(indicator_cfg=indicator_cfg),
                timeout=300,
            )
            logger.info(f"Transformation succeeded for {indicator_id}")
            processing_statuses["transformed_indicators"].append(indicator_id)
        except (TimeoutError, asyncio.TimeoutError, Exception):
            logger.error(f"Timeout while processing {indicator_id}")
            processing_statuses["failed_indicators"].append(indicator_id)


@log_exceptions
async def transform_sources(
    indicator_ids: list = None,
    indicator_id_contain_filter: str = None,
    concurrent_chunk_size: int = 5,
) -> list[str] | None:
    """
    Perform transformations for a list of indicators.
    """
    processing_statuses = {"failed_indicators": [], "transformed_indicators": []}

    semaphore = asyncio.Semaphore(concurrent_chunk_size)

    async with StorageManager() as storage_manager:
        indicators_cfgs = await storage_manager.get_indicators_cfg(
            indicator_ids=indicator_ids, contain_filter=indicator_id_contain_filter
        )

        logger.debug(f"Retrieved {len(indicators_cfgs)} indicators")
        if not indicators_cfgs:
            logger.info(
                f"No indicators retrieved using indicator_ids={indicator_ids} "
                f"and indicator_id_contain_filter={indicator_id_contain_filter}"
            )
            return None

        tasks = [
            process_indicator(cfg, semaphore, processing_statuses)
            for cfg in indicators_cfgs
        ]
        await asyncio.gather(*tasks)

        logger.info("#" * 100)
        logger.info(f"TASKED: {len(indicators_cfgs)} indicators")
        logger.info(
            f"TRANSFORMED: {len(processing_statuses['transformed_indicators'])} indicators"
        )
        logger.info(
            f"FAILED: {len(processing_statuses['failed_indicators'])} indicators"
        )
        logger.info("#" * 100)

        return (
            processing_statuses["transformed_indicators"]
            if processing_statuses["transformed_indicators"]
            else None
        )


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
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

    transformed_sources = asyncio.run(transform_sources())
    print(transformed_sources)
