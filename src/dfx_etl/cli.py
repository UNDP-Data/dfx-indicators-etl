import argparse
import logging
import os.path
import sys
from dfx_etl.settings import SETTINGS
from dfx_etl.pipelines import Pipeline, get_pipeline, list_pipelines
logger = logging.getLogger(__name__)


STEPS = ["retrieve", "transform", "load"]




class Formatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )



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
    parser = build_parser()

    if argv is None:
        argv = sys.argv[1:]

    if not argv:
        parser.print_help()
        return 1

    args = parser.parse_args(argv)


    # validate

    dst_folder = args.dst_folder

    if dst_folder is not None:
        if not os.path.isabs(dst_folder):
            dst_folder = os.path.abspath(dst_folder)
        if not os.path.exists(dst_folder):
            os.makedirs(dst_folder)
        SETTINGS.local_storage = dst_folder
    print(SETTINGS)
    azure_path = None
    if azure_path is not None:
        if not '/' in azure_path:
           parser.error(f'--azure-path={azure_path} is not a valid Azure path')






    for src in args.src:
        pipeline = get_pipeline(src)
        match args.step:
            case 'retrieve':
                return pipeline.retrieve()
            case 'transform':
                return pipeline.transform()
            case 'load':
                return pipeline.load()
            case _:
                return pipeline()






    # run

    return 0


if __name__ == "__main__":

    configure_logging("INFO")
    raise SystemExit(main(sys.argv))
