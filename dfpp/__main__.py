import logging
import asyncio
import argparse
from asyncio import sleep
import sys
from dotenv import load_dotenv
from dfpp.download import retrieval
from dfpp.constants import *
from dfpp.run_transform import transform_sources

parser = argparse.ArgumentParser()
parser.add_argument('--run',
                    help='The function to run. options are download, standardise, and publish, or all of the functions together like pipeline')
parser.add_argument('--env', help='Path to the .env file')


def run_pipeline():
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
    logger.name = __name__
    asyncio.run(main())
async def main():
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    if args.env:
        load_dotenv(dotenv_path=args.env)
        connection_string = AZURE_STORAGE_CONNECTION_STRING
        container_name = AZURE_STORAGE_CONTAINER_NAME
        if args.run == 'download':
            await retrieval(connection_string=connection_string, container_name=container_name)
        if args.run == 'transform':
            await transform_sources()
        if args.run == 'pipeline':
            logging.info('Starting pipeline....')
            await sleep(5)
            logging.info('Downloading data....')
            await retrieval(connection_string=connection_string, container_name=container_name)
            logging.info('Downloading Data Complete....')
            logging.info('Transforming data....')
            await sleep(5)
            await transform_sources()
            logging.info('Transforming Data Complete....')
    else:
        raise Exception('Environment Variable Not Found')


if __name__ == '__main__':
    run_pipeline()

