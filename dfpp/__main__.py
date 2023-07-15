import logging
import asyncio
import argparse
import os
from asyncio import sleep
import sys
from dfpp.download import retrieval
from dfpp.publish import publish
from dfpp.run_transform import transform_sources

parser = argparse.ArgumentParser()
parser.add_argument('--run',
                    help='The function to run. options are download, transform, and publish, or all of the functions together like `pipeline`')



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

def check_evars(cfg, env_file):

    for k, v in cfg.items():
        assert k in cfg, f'"{k}" env. variable is not set in {env_file}'
        v = cfg[k]
        assert v not in ['', None] , f'"k"={v} is  invalid'


async def main():
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    container_name = os.environ.get('AZURE_STORAGE_CONTAINER_NAME')
    if args.run == 'download':
        await retrieval(connection_string=connection_string, container_name=container_name)
    if args.run == 'transform':
        await transform_sources(concurrent=True)
    if args.run == 'publish':
        await publish()
    if args.run == 'pipeline':
        logging.info('Starting pipeline....')
        await sleep(5)
        logging.info('Downloading data....')
        downloaded_indicator_ids = await retrieval(connection_string=connection_string, container_name=container_name)
        logging.info('Downloading Data Complete....')
        logging.info('Transforming data....')
        await sleep(5)
        await transform_sources()
        logging.info('Transforming Data Complete....')

        #TODO report fucntion
if __name__ == '__main__':
    run_pipeline()

