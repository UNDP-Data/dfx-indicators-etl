import json
import os
from dfpp.storage import StorageManager
import asyncio
import logging
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
        soource_ids = [e['indicator']['source_id'] for e in indicators_cfg]
        logger.info(soource_ids)
        sources_cfg = await sm.get_sources_cfg(source_ids=soource_ids)
        for e in sources_cfg:
            logger.info(json.dumps(e))
    logger.info('disconnected')



asyncio.run(main())