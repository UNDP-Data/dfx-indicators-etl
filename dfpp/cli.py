import argparse
import asyncio
import logging
import sys

from io import StringIO
from traceback import print_exc
import os
from dotenv import load_dotenv

from dfpp.backup_pipeline import backup_pipeline, backup_raw_sources, backup_base_files

load_dotenv()
from functools import partial, partialmethod

logging.TRACE = 5
logging.addLevelName(logging.TRACE, 'TRACE')
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
logging.trace = partial(logging.log, logging.TRACE)


def run_pipeline():
    asyncio.run(main())


def validate_env():
    if os.environ.get('AZURE_STORAGE_CONNECTION_STRING') is None:
        raise Exception('AZURE_STORAGE_CONNECTION_STRING is not set')
    if os.environ.get('AZURE_STORAGE_CONTAINER_NAME') is None:
        raise Exception('AZURE_STORAGE_CONTAINER_NAME variable is not set')
    if os.environ.get('ROOT_FOLDER') is None:
        raise Exception('ROOT_FOLDER is not set')
    # if os.environ.get('POSTGRES_DSN') is None:
    #     raise Exception('POSTGRES_DSN is not set')


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

    parser = argparse.ArgumentParser(description='Data Futures Platform pipeline command line script.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     )
    parser.add_argument('-l', '--log-level', help='Set log level to ', type=str, choices=['INFO', 'DEBUG', 'TRACE'],
                        default='INFO')
    parser.add_argument('--no-cache', help='Do not use cache', action='store_true')
    # parser.add_argument('-p', '--process_indicators',
    #                     help='The indicator/s to process. If not supplied all detected indicators '
    #                          'will be processed. Applies to all subcommands except list',
    #                     nargs='+')
    parser.add_argument('-e', '--load-env-file',
                        help='search for a .env file containing environment variables ',
                        action='store_true')

    subparsers = parser.add_subparsers(help='main commands', dest='command', required=True)
    list_parser = subparsers.add_parser(name='list', help='List pipeline info and/or configuration',
                                        description='List pipeline info and/or configuration ')
    list_parser.add_argument('-i', '--indicators', help='List available indicators', action='store_true')
    list_parser.add_argument('-s', '--sources', help='List available sources', action='store_true')
    list_parser.add_argument('-c', '--config', help='List available configuration', action='store_true')

    run_parser = subparsers.add_parser(
        name='run',
        help='Run specific stages or the whole pipeline',
        description='Run specific states or the whole pipeline'
    )
    run_parser.add_argument('-i', '--indicator_ids',
                            help='The id of indicator/s to process. If not supplied all detected indicators '
                                 'will be processed',
                            nargs='+')

    run_parser.add_argument('-f', '--filter-indicators-string',
                            help='Process only indicators whose id contains this string', type=str)

    stage_subparsers = run_parser.add_subparsers(help='pipeline stages', dest='stage')

    # #list_parser.set_defaults(func=await list_command)
    download_parser = stage_subparsers.add_parser(
        name='download',
        help='Download indicators source raw data',
        description='Download data sources for specific indicators as defined in the configuration files.'
                    'The downloaded raw source files are uploaded to Azure'
    )
    download_parser.add_argument('-i', '--indicator_ids',
                                 help='The id of indicator/s to process. If not supplied all detected indicators '
                                      'will be processed',
                                 nargs='+')

    download_parser.add_argument('-f', '--filter-indicators-string',
                                 help='Process only indicators whose id contains this string', type=str)

    transform_parser = stage_subparsers.add_parser(
        name='transform',
        help='Transform/homogenize specific indicators',
        description='Transform/homogenize specific indicators and upload the results into base files'
    )

    transform_parser.add_argument('-i', '--indicator_ids',
                                  help='The id of indicator/s to process. If not supplied all detected indicators '
                                       'will be processed',
                                  nargs='+')

    transform_parser.add_argument('-f', '--filter-indicators-string',
                                  help='Process only indicators whose id contains this string', type=str)

    publish_parser = stage_subparsers.add_parser(
        name='publish',
        help='Publish/upload specific indicators to the specified end point',
        description='Publish/upload specific indicators to the specified end point'
    )

    publish_parser.add_argument('-i', '--indicator_ids',
                                help='The id of indicator/s to process. If not supplied all detected indicators '
                                     'will be processed',
                                nargs='+')

    publish_parser.add_argument('-f', '--filter-indicators-string',
                                help='Process only indicators whose id contains this string', type=str)

    # display help by default
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    if args.log_level:
        logger.setLevel(args.log_level)
    if args.load_env_file is True:
        load_dotenv()
    ## required
    validate_env()
    """
    These import are here in case the 
    """
    from dfpp.download import download_indicator_sources
    from dfpp.publishpg import publish
    # from dfpp.publish_new import publish
    from dfpp.run_transform import transform_sources
    from dfpp.storage import TMP_SOURCES
    from dfpp.utils import list_command

    if args.no_cache:
        TMP_SOURCES.clear()
    try:
        if args.command == 'list':
            if not (args.indicators or args.sources or args.config):
                parser = locals()[f'{args.command}_parser']
                parser.parse_args(['--help'])
            await list_command(
                indicators=args.indicators,
                sources=args.sources,
                config=args.config
            )
        if args.command == 'run':
            validate_env()
            # await backup_raw_sources()
            if args.stage == 'download':
                downloaded_indicators = await download_indicator_sources(
                    indicator_ids=args.indicator_ids,
                    indicator_id_contain_filter=args.filter_indicators_string
                )
            # await backup_base_files()
            if args.stage == 'transform':
                transformed_indicators = await transform_sources(
                    indicator_ids=args.indicator_ids,
                    indicator_id_contain_filter=args.filter_indicators_string,
                    project='access_all_data'
                )
                print(transformed_indicators)
            if args.stage == 'publish':
                published_indicators = await publish(
                    indicator_ids=args.indicator_ids,
                    indicator_id_contain_filter=args.filter_indicators_string,
                    project='access_all_data'

                )
            if args.stage is None:
                # await backup_pipeline()
                downloaded_indicators = await download_indicator_sources(
                    indicator_ids=args.indicator_ids,
                    indicator_id_contain_filter=args.filter_indicators_string
                )
                transformed_indicators = await transform_sources(
                    indicator_ids=downloaded_indicators,
                    project='access_all_data'
                )
                published_indicators = await publish(
                    indicator_ids=transformed_indicators,
                    project='access_all_data'
                )

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
    asyncio.run(main())
