import argparse
import logging
import os.path
import sys
from dfx_etl.settings import SETTINGS
from dfx_etl.pipelines import  get_pipeline, list_pipelines
from pathlib import Path
from inspect import signature
from dfx_etl.storage import LocalStorage

STEPS = ["retrieve", "transform", "load"]


def setup_logs(level=None):
    #azlogger = logging.getLogger('az.core.pipeline.policies.http_logging_policy')
    azlogger = logging.getLogger('azure')
    azlogger.setLevel(logging.WARNING)
    httpx_logger = logging.getLogger('httpx')
    httpx_logger.setLevel(logging.WARNING)


class Formatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass


def configure_logging() -> logging.Logger:
    logger = logging.getLogger()

    # Avoid duplicate handlers if main() is called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s",
            "%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(handler)
        logger.propagate = False
    logger.name = __name__
    logger.setLevel(logging.INFO)
    return logger



EPILOG = f"""
        Tips: 
            omit --src to execute all pipelines: {', '.join(list_pipelines())}
            omit --step to execute all steps: {', '.join(STEPS)}
        Usage:
            one
            two
        """

def build_parser() -> argparse.ArgumentParser:
    """
    Build an argparse parser suitable ot launch pipelines from command line
    Returns
    -------

    """
    parser = argparse.ArgumentParser(
        prog="dfx-etl",
        description="Run dfx_etl pipelines and store data  locally or on Azure ",
        formatter_class=Formatter,
        epilog=EPILOG
    )

    parser.add_argument(
        "--src",
        type=str,
        required=False,
        nargs='+',
        choices=list_pipelines(),
        metavar="SOURCE",
        help="Execute DFx pipeline on one or multiple sources. (choices: %(choices)s)",
    )

    parser.add_argument(
        "--step",
        type=str,
        required=False,
        choices=STEPS,
        metavar="STEP",
        help="The step of the specified pipeline (choices: %(choices)s)",
    )

    dest = parser.add_mutually_exclusive_group(required=True)

    dest.add_argument(
        "--dst-folder",
        type=str,
        metavar='PATH',
        help="A path to a directory on the local disk. The storage where the intermediary parquet files will be stored. ",
    )

    dest.add_argument(
        "--azure-path",
        type=str,
        metavar='CONTAINER[/PATH]',
        help="A relative path to an directory in Azure. Needs to start with container name. ex: dfxdata/indicators, dfx/indicators/prod",
    )

    parser.add_argument(
        "-d", "--debug", help="Enable debug logging", action="store_true", default=False
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    setup_logs()
    parser = build_parser()

    if argv is None:
        argv = sys.argv[1:]

    if not argv:
        parser.print_help()
        return 1

    args = parser.parse_args(argv)
    logger = configure_logging()
    # validate
    dst_folder = args.dst_folder
    if dst_folder is not None:
        if not os.path.isabs(dst_folder):
            dst_folder = os.path.abspath(dst_folder)
        if not os.path.exists(dst_folder):
            os.makedirs(dst_folder)

    azure_path = None
    if args.azure_path is not None:
        if not '/' in azure_path:
           parser.error(f'--azure-path={azure_path} is not a valid Azure path')
    if args.debug:
        logger.setLevel(logging.DEBUG)

    for src in args.src:
        pipeline = get_pipeline(src)
        print(pipeline, args.step)



        if args.step == 'retrieve':
            #needs_storage = "storage" in signature(pipeline.retriever).parameters
            pass

        match args.step:
            case 'retrieve':
                pipeline.retrieve()
                num_rows, num_cols = pipeline.df_raw.shape
                if num_rows == 0 or num_cols == 0:
                    logger.warning(f'The source data frame is empty! Please check {pipeline.retriever.uri}')
                    return 1
                if dst_folder is not None:
                    SETTINGS.local_storage = Path(dst_folder)
                    local_storage = LocalStorage()
                    raw_parquet_file = local_storage.write_dataset(pipeline.df_raw, folder_path=dst_folder)
                    logger.info(f'{num_rows} rows and {num_cols} columns worth of raw data was written to {raw_parquet_file}')
                    SETTINGS.local_storage = None
                return 0
            case 'transform':
                pipeline.retrieve().transform()
                num_rows, num_cols = pipeline.df_transformed.shape
                if num_rows == 0 or num_cols == 0:
                    logger.warning(f'The transformed data frame is empty! Please check {pipeline.retriever.uri}')
                    return 1
                if dst_folder is not None:
                    SETTINGS.local_storage = Path(dst_folder)
                    local_storage = LocalStorage()
                    transformed_parquet_file = local_storage.write_dataset(pipeline.df_transformed, folder_path=dst_folder)
                    logger.info(
                        f'{num_rows} rows and {num_cols} columns worth of transformed data was written to {transformed_parquet_file}')
                    SETTINGS.local_storage = None
                return 0
            case 'load':
                return pipeline.load()
            case _:
                return pipeline()


    return 0


