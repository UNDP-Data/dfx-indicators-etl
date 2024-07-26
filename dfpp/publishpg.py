"""
Functions to publish indicators to PostgreSQL
"""
import asyncio
import io
import logging
import os
from traceback import print_exc
from typing import List

import pandas as pd

from . import constants
from .exceptions import AggregationError, PublishError
from .storage import StorageManager
from .utils import base_df_for_indicator, chunker

logger = logging.getLogger(__name__)
project = 'access_all_data'
output_data_type = 'timeseries'


async def publish_indicator(
        storage_manager: StorageManager,
        indicator_id: str = None,
        drop_null: bool = False,
):
    """
    Publish the indicator to the Data Futures Platform.

    This function reads the base file for the given indicator ID, constructs an indicator DataFrame with
    'year,' 'indicator_id,' and 'value' columns, and returns it.
    If drop_null is True, rows with missing values
    (NaN) will be dropped from the DataFrame.

    :param storage_manager: The StorageManager object responsible for handling file operations.
    :param indicator_id: The ID of the indicator to be published.
    :param drop_null: A boolean flag to specify whether to drop rows with missing values (default: False).
    :return: pd.DataFrame or None: The pandas DataFrame containing the indicator data if successful,
                                   otherwise None if an error occurs.
    """
    assert indicator_id is not None, "Indicator id is required"
    indicator_df = pd.DataFrame(columns=['year', 'indicator_id', 'value'])
    try:
        logger.info(f'Publishing indicator_id {indicator_id}')
        # Retrieve the base DataFrame for the indicator
        base_df = await base_df_for_indicator(storage_manager=storage_manager, indicator_id=indicator_id,
                                              project=project)
        # Extract indicator columns and years from the base DataFrame
        indicator_columns = [column for column in base_df.columns if indicator_id in column]
        years = [int(column.split('_')[-1]) for column in indicator_columns]

        if len(years) == 0:
            # Log a warning if no years are found for the indicator
            logger.error(f'No data found for indicator_id {indicator_id}')
            raise PublishError(f'No publish-able data found for indicator_id {indicator_id}')

        for year in years:
            # Extract data for the specific year and construct the year_df
            year_df = base_df[[constants.STANDARD_KEY_COLUMN] + [f'{indicator_id}_{year}']]
            year_df = year_df.rename(columns={f'{indicator_id}_{year}': 'value'})
            year_df['year'] = year
            year_df['indicator_id'] = indicator_id
            indicator_df = pd.concat([indicator_df, year_df])
        indicator_df.rename(columns={constants.STANDARD_KEY_COLUMN: 'country_iso3'}, inplace=True)
        indicator_df = indicator_df[['indicator_id', 'country_iso3', 'year', 'value']]
        if drop_null:
            # Drop rows with missing values if drop_null is True
            indicator_df = indicator_df.dropna()
        # At last, aggregate the indicator
        # TODO: Disabled aggregation as it is going to be done in the API side
        # indicator_aggregate_json = await aggregate_indicator(
        #     project=project,
        #     indicator_id=indicator_id,
        # )
        indicator_json = indicator_df.to_json(orient='records')

        with open(f'/home/thuha/Desktop/UNDP/dfp/dv-data-pipeline/outputs/{indicator_id}.json', 'w') as f:
            f.write(indicator_json)

        # json.dump(indicator_json, open(f'{indicator_id}.json', 'w'), indent=4)

        # with open(f'/home/thuha/Desktop/UNDP/dfp/dv-data-pipeline/dfpp/output/per_indicator/{indicator_id}_aggregates.json', 'w') as fa:
        #     fa.write(indicator_aggregate_json)
        # TODO: Upload the indicator_json and indicator_aggregate_json to the URL
        return indicator_id
    except AggregationError as ae:
        logger.error(f'Failed to aggregate indicators with error={ae}')
        return indicator_id
    except Exception as e:
        raise e


async def publish(
        indicator_ids: List[str] = None,
        indicator_id_contain_filter: str = None,
        project: str = None,
        concurrent_chunk_size: int = 50,
) -> List[str]:
    """
    Publish the Data Futures Platform indicator/s to PostGRES.

    This function reads the base file for each indicator in the specified indicator_ids list or that
    contains the specified string in its ID, constructs an indicator DataFrame, and appends it to the
    output DataFrame. The output DataFrame is then uploaded to the specified project folder in the
    Data Futures Platform.

    :param concurrent_chunk_size: Size of chunks to process concurrently.
    :param indicator_ids: A list of indicator IDs to publish (optional).
    :param indicator_id_contain_filter: A string that the indicator ID should contain (optional).
    :param project: The project to which the indicators will be published. It Must be one of
                    'access_all_data' or 'vaccine_equity.'
    :return: List[str]: A list of indicator IDs that were successfully processed and published.
    """
    assert project in ['access_all_data', 'vaccine_equity'], "Project must be one of access_all_data or vaccine_equity"
    dsn = os.environ.get('POSTGRES_DSN')
    try:

        failed_indicators = []
        processed_indicators = []
        async with StorageManager() as storage_manager:
            indicator_cfgs = await storage_manager.get_indicators_cfg(
                indicator_ids=indicator_ids,
                contain_filter=indicator_id_contain_filter
            )

            # Retrieve indicator configurations based on indicator_ids or the indicator_id_contain_filter
            indicator_ids = [cfg['indicator']['indicator_id'] for cfg in indicator_cfgs]

            for chunk in chunker(indicator_ids, size=concurrent_chunk_size):
                tasks = []
                for indicator_id in chunk:
                    tasks.append(
                        publish_indicator(
                            storage_manager=storage_manager,
                            indicator_id=indicator_id,
                            drop_null=False
                        )
                    )
                for task in asyncio.as_completed(tasks, timeout=60 * len(tasks)):
                    try:
                        processed_indicators.append(await task)
                    except Exception as e:
                        failed_indicators.append(indicator_id)
                        with io.StringIO() as m:
                            print_exc(file=m)
                            em = m.getvalue()
                            logger.error(f'Error {em} was encountered while publishing {indicator_id}')
                        continue

            # logger.info(f'Finished uploading output file to project {project}')
            print(processed_indicators)
            logger.info(f'Published {len(processed_indicators)} indicators to project {project}')
            if failed_indicators:
                logger.info(f'Failed to publish {len(failed_indicators)} indicators to project {project}')
            return processed_indicators
    except Exception as e:
        raise PublishError(f'Failed to publish indicators with error={e}')


if __name__ == "__main__":
    pass
