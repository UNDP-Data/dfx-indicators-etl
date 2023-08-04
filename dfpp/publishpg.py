"""
Functions to publish indicators to PostgreSQL
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

logger = logging.getLogger(__name__)
# project = 'vaccine_equity'
project = 'access_all_data'
output_data_type = 'timeseries'


async def publish_indicator(base_file=None, indicator_id=None, outdf=None ):
    """
    group by years and add yeach year to out
    :param base_file:
    :param indicator_id:
    :param outdf:
    :return:
    """
    pass
async def publish(
        indicator_ids: list,
        indicator_id_contain_filter: str = None,
        project=None
    )-> List[str]:
    """
    Publish the data to the Data Futures Platform
    :return:
    """
    assert project not in ['', None], f'Invalid project={project}.'

    async with StorageManager() as storage_manager:
        logger.debug("Connected to Azure Blob Storage")
        logger.info("Starting to read indicator configurations...")
        # indicator_cfgs = await storage_manager.get_indicators_cfg(
        #     indicator_ids=indicator_ids,
        #     contain_filter=indicator_id_contain_filter
        # )
        # source_configs = await storage_manager.get_sources_cfgs()
        #out_data_frame = pd.DataFrame (4 cols)
        # for chunk in indicators
            #for every indicator in chunk
                # 1. read the base file for indicator
                # 2 group by years
                # for every yyear:
                    # add (alpha3, year, indicator_id, value) to outdf
        # sort the outdf uinsg indicator and year
        # SAVE TO POSTGRES



        # indicator_cfgs = await storage_manager.get_indicators_cfg(
        #     indicator_ids=indicator_ids,
        #     contain_filter=indicator_id_contain_filter
        # )
        # source_configs = await storage_manager.get_sources_cfgs()