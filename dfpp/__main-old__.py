import argparse
import asyncio
import logging
import os
import sys
from asyncio import sleep
from distutils.util import strtobool
from io import StringIO
from traceback import print_exc

from dfpp.download import download_indicator_sources
from dfpp.publish import publish
from dfpp.run_transform import transform_sources
from dfpp.storage import TMP_SOURCES

parser = argparse.ArgumentParser(
    description='Convert layers/bands from GDAL supported geospatial data files to COGs/PMtiles.',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-s', '--stage',
                    help='The stage to run. Options are: download, transform, and publish')
parser.add_argument('-i', '--indicators', help='The indicator/s to process. If not supplied all detected indicators '
                                               'will be processed',
                    nargs='+')
parser.add_argument('-f', '--filter-indicators',
                    help='Process only indicators whose id contains this string')
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
    logger.name = 'dfpp'

    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    if args.debug is True:
        logger.setLevel(logging.DEBUG)
    indicators_from_args = args.indicators
    indicators_from_args_contains = args.filter_indicators
    validate_env()
    try:
        if args.stage == 'download':
            await download_indicator_sources(
                indicator_ids=indicators_from_args,
                indicator_id_contain_filter=indicators_from_args_contains
            )
        elif args.stage == 'transform':
            await transform_sources(concurrent=True, indicator_ids=indicators_from_args, project="access_all_data")
        elif args.stage == 'publish':
            await publish()
        else:
            logging.info('Starting pipeline....')
            await sleep(1)
            logger.info('Downloading data....')
            downloaded_indicator_ids = await download_indicator_sources(indicator_ids=indicators_from_args)
            logger.info('Downloading Data Complete....')
            logger.info('Transforming data....')
            await sleep(1)
            await transform_sources(concurrent=True, indicator_ids=downloaded_indicator_ids,
                                    project="access_all_data")
            logger.info('Transforming Data Complete....')
            # logger.info('Publishing data....')
            # await sleep(1)
            # await publish(indicator_ids=transformed_indicator_ids)
            # logger.info('Publishing Data Complete....')


    except Exception as e:
        with StringIO() as m:
            print_exc(file=m)
            em = m.getvalue()
            logger.error(em)

    finally:
        for k, v in TMP_SOURCES.items():
            exists = os.path.exists(v)
            if exists:
                logger.debug(f'Removing cache {v} for {k} ')
                os.remove(v)


if __name__ == '__main__':
    run_pipeline()
