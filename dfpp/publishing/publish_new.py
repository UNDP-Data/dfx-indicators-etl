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

from ..constants import STANDARD_KEY_COLUMN
from ..exceptions import PublishError
from ..storage import StorageManager
from ..utils import chunker

logger = logging.getLogger(__name__)
project = "access_all_data"
output_data_type = "timeseries"


async def base_df_for_indicator(
    storage_manager: StorageManager, indicator_id: str
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
    base_file_df = pd.read_csv(
        io.BytesIO(await storage_manager.cached_download(source_path=base_file_path))
    )

    return base_file_df


async def publish_indicator(
    storage_manager: StorageManager, indicator_id: str = None, drop_null: bool = False
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
    indicator_df = pd.DataFrame(columns=["year", "indicator_id", "value"])

    try:
        logger.info(f"Publishing indicator_id {indicator_id}")
        # Retrieve the base DataFrame for the indicator
        base_df = await base_df_for_indicator(
            storage_manager=storage_manager, indicator_id=indicator_id
        )

        # Extract indicator columns and years from the base DataFrame
        indicator_columns = [
            column for column in base_df.columns if indicator_id in column
        ]
        years = [int(column.split("_")[-1]) for column in indicator_columns]

        if len(years) == 0:
            # Log a warning if no years are found for the indicator
            logger.warning(f"No data found for indicator_id {indicator_id}")
            return None

        for year in years:
            # Extract data for the specific year and construct the year_df
            year_df = base_df[[STANDARD_KEY_COLUMN] + [f"{indicator_id}_{year}"]]
            year_df = year_df.rename(columns={f"{indicator_id}_{year}": "value"})
            year_df["year"] = year
            year_df["indicator_id"] = indicator_id
            indicator_df = pd.concat([indicator_df, year_df])

        if drop_null:
            # Drop rows with missing values if drop_null is True
            indicator_df = indicator_df.dropna()

        return indicator_df

    except Exception as e:
        # Print the exception traceback and log the error
        print_exc()
        logger.error(f"Failed to publish indicator_id={indicator_id} with error={e}")
        return None


async def publish(
    indicator_ids: List[str] = None,
    indicator_id_contain_filter: str = None,
    project: str = None,
) -> List[str]:
    """
    Publish the data to the Data Futures Platform.

    This function reads the base file for each indicator in the specified indicator_ids list or that
    contains the specified string in its ID, constructs an indicator DataFrame, and appends it to the
    output DataFrame. The output DataFrame is then uploaded to the specified project folder in the
    Data Futures Platform.

    :param indicator_ids: A list of indicator IDs to publish (optional).
    :param indicator_id_contain_filter: A string that the indicator ID should contain (optional).
    :param project: The project to which the indicators will be published. Must be one of
                    'access_all_data' or 'vaccine_equity.'
    :return: List[str]: A list of indicator IDs that were successfully processed and published.
    """
    assert project in [
        "access_all_data",
        "vaccine_equity",
    ], "Project must be one of access_all_data or vaccine_equity"
    try:
        skipped_indicators = []
        failed_indicators = []
        processed_indicators = []

        async with StorageManager() as storage_manager:
            indicator_cfgs = await storage_manager.get_indicators_cfg(
                indicator_ids=indicator_ids, contain_filter=indicator_id_contain_filter
            )

            output_dataframe = pd.DataFrame(
                columns=[STANDARD_KEY_COLUMN, "year", "indicator_id", "value"]
            )
            # Retrieve indicator configurations based on indicator_ids or the indicator_id_contain_filter
            indicator_ids = [cfg["indicator"]["indicator_id"] for cfg in indicator_cfgs]

            # Process indicators in chunks to avoid overwhelming resources
            for chunk in chunker(indicator_ids, 50):
                tasks = []
                for indicator_id in chunk:
                    tasks.append(
                        publish_indicator(
                            storage_manager=storage_manager, indicator_id=indicator_id
                        )
                    )
                for task in asyncio.as_completed(tasks, timeout=30 * len(tasks)):
                    try:
                        indicator_df = await task
                        if indicator_df is None:
                            skipped_indicators.append(indicator_id)
                            continue
                    except Exception as e:
                        failed_indicators.append(indicator_id)
                        with io.StringIO() as m:
                            print_exc(file=m)
                            em = m.getvalue()
                            logger.error(
                                f"Error {em} was encountered while publishing {indicator_id}"
                            )
                        continue
                    processed_indicators.append(indicator_id)
                    output_dataframe = pd.concat([output_dataframe, indicator_df])

            # Upload the output DataFrame to the specified project folder
            logger.info(f"Uploading output file to project {project}")
            await storage_manager.upload(
                data=output_dataframe.to_csv(index=False).encode("utf-8"),
                dst_path=os.path.join(
                    storage_manager.output_path, project, "output_test.csv"
                ),
                content_type="text/csv",
                overwrite=True,
            )
            logger.info(f"Finished uploading output file to project {project}")
            logger.info(
                f"Published {len(processed_indicators)} indicators to project {project}"
            )
            logger.info(
                f"Skipped {len(skipped_indicators)} indicators to project {project}"
            )
            logger.info(
                f"Failed to publish {len(failed_indicators)} indicators to project {project}"
            )
            # Return the list of successfully processed and published indicator IDs
            return processed_indicators
    except Exception as e:
        raise PublishError(f"Failed to publish indicators with error={e}")


if __name__ == "__main__":
    pass
