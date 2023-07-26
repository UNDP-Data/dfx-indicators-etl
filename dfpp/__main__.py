import logging
import asyncio
import argparse
import os
from asyncio import sleep
import sys
from dfpp.download import download_indicator_sources
from dfpp.publish import publish
from dfpp.run_transform import transform_sources
from dfpp.storage import TMP_SOURCES
from  distutils.util import strtobool
from io import  StringIO
from traceback import  print_exc

parser = argparse.ArgumentParser(description='Convert layers/bands from GDAL supported geospatial data files to COGs/PMtiles.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--run',
                    help='The function to run. options are download, transform, and publish, or all of the functions together like `pipeline`')
parser.add_argument('-i', '--indicators', help='The indicator to process. options are all, or a specific indicator like `GDP`',
                    nargs='+')
parser.add_argument('-f', '--filter-indicators',
                    help='The indicator to run. options are all, or a specific indicator like `GDP`')
parser.add_argument('-d', '--debug', type=strtobool,
                    help='Set log level to debug', default=False
                    )

def run_pipeline():

    asyncio.run(main())


def validate_env():
    if os.environ.get('AZURE_STORAGE_CONNECTION_STRING') is None:
        raise Exception('AZURE_STORAGE_CONNECTION_STRING is not set')
    if os.environ.get('AZURE_STORAGE_CONTAINER_NAME') is None:
        raise Exception('AZURE_STORAGE_CONTAINER_NAME variable is not set')
    if os.environ.get('ROOT_FOLDER') is None:
        raise Exception('ROOT_FOLDER is not set')


async def main():
    logging.basicConfig()
    azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
    azlogger.setLevel(logging.WARNING)
    logger = logging.getLogger()
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
    #logger.name = __name__

    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    if args.debug is True:
        logger.setLevel(logging.DEBUG)
    indicators_from_args = args.indicators
    indicators_from_args_contains = args.filter_indicators
    validate_env()
    try:
        if args.run == 'download':
            logger.name = 'downloader'
            downloaded_indicators = await download_indicator_sources(
                indicator_ids=indicators_from_args,
                indicator_id_contain_filter=indicators_from_args_contains
            )
        if args.run == 'transform':
            await transform_sources(concurrent=True, indicator_ids=indicators_from_args)
        if args.run == 'publish':
            await publish()
        if args.run == 'pipeline':
            logging.info('Starting pipeline....')
            await sleep(5)
            logger.info('Downloading data....')
            downloaded_indicator_ids = await download_indicator_sources(indicator_ids=indicators_from_args)
            logger.info('Downloading Data Complete....')
            logger.info('Transforming data....')
            await sleep(5)
            transformed_indicator_ids = await transform_sources(indicator_ids=downloaded_indicator_ids)
            logger.info('Transforming Data Complete....')
            logger.info('Publishing data....')
            await sleep(5)
            await publish(indicator_ids=transformed_indicator_ids)
            logger.info('Publishing Data Complete....')
            # TODO report function

            # todo clear cache

    except Exception as e:
        with StringIO() as m:
            print_exc(file=m)
            em = m.getvalue()
            logger.error(em)
        logger.error(f'{args.run} failed with exception "{e}"')
    finally:
        for k, v in TMP_SOURCES.items():
            exists = os.path.exists(v)
            if exists:
                logger.debug(f'Removing cache {v} for {k} ')
                os.remove(v)


if __name__ == '__main__':
    run_pipeline()
