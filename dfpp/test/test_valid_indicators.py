import asyncio
import logging
from dotenv import load_dotenv
load_dotenv(dotenv_path='../../.env')
from dfpp.storage import StorageManager
from dfpp.run_transform import run_transformation_for_indicator
from io import StringIO
from traceback import print_exc
async def main():


    skipped_or_failed = list()
    transformed_indicators = list()
    indicator_cfgs = {}
    async with StorageManager() as sm:
        indicator_cfgs = await sm.get_indicators_cfg()


        for indicator_cfg in indicator_cfgs:

            indicator_section = indicator_cfg['indicator']
            indicator_id = indicator_section['indicator_id']
            if indicator_section.get('preprocessing') is None:
                # Skip if no preprocessing function is specified
                logger.info(
                    f"Skipping preprocessing for indicator {indicator_id} as no preprocessing function is specified")
                continue

            try:
                # Perform transformation sequentially
                transformed_indicator_id = await run_transformation_for_indicator(indicator_cfg=indicator_section,
                                                                                  project='access_all_data')
                transformed_indicators.append(transformed_indicator_id)

            except Exception as te:
                skipped_or_failed.append(indicator_id)
                with StringIO() as m:
                    print_exc(file=m)
                    em = m.getvalue()
                    logger.error(f'Error {em} was encountered while processing  {indicator_id}')
                    continue

    logger.info(f'Total no of indicators: {len(indicator_cfgs)}')
    logger.info(f'Skipped or failed no of indicators: {len(skipped_or_failed)}')
    logger.info(f'Processed no of indicators: {len(transformed_indicators)}')


if __name__ == '__main__':


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

    asyncio.run(main())