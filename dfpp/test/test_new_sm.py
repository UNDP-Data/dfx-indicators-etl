import asyncio
import json
import logging
import os

from ..storage import StorageManager

azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
azlogger.setLevel(logging.WARNING)
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
container_name = os.environ.get('AZURE_STORAGE_CONTAINER_NAME')


async def main():
    async with StorageManager(
                connection_string=connection_string,
                container_name=container_name,
            ) as sm:

        logger.info(f'connected to azure')
        logger.info(str(sm))

        indicators_cfg = await sm.get_indicators_cfg(contain_filter='wb')
        indicator_ids = [e['indicator']['indicator_id'] for e in indicators_cfg]
        source_ids = [e['indicator']['source_id'] for e in indicators_cfg]




        sources_cfg = await sm.get_sources_cfg(source_ids=source_ids)
        downloaded_source_ids = [e['source']['id'] for e in sources_cfg]
        downloaded_indicator_ids = [e['indicator']['indicator_id'] for e in indicators_cfg if
                                    e['indicator']['source_id'] in downloaded_source_ids]
        logger.info(f'{len(source_ids)}, {len(indicator_ids)} {len(downloaded_indicator_ids)}')
        # from .run_transform import transform_sources

        #transform_sources(indicator_ids=indicator_ids)
        for e in sources_cfg:
            logger.info(json.dumps(e))


    logger.info('disconnected')



asyncio.run(main())