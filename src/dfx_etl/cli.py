import argparse
import logging
import os.path
import sys
import pkgutil
import importlib
from dfx_etl.pipelines import Pipeline
from dfx_etl.storage import get_storage
logger = logging.getLogger(__name__)


STEPS = ["retrieve", "transform", "load"]
DESTINATIONS = ['local', 'azure']



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


def get_available_sources() -> dict[str:str]:
    """
    Fetches all public python modules located in "dfx_etl.pipelines" submodule, that is
    they do not start or end with "_"
    Returns, list of str representing pipelines FQDN
    -------

    """
    pkg = importlib.import_module(f'{__package__}.pipelines')
    modules = {}

    for module_info in pkgutil.iter_modules(pkg.__path__):
        if module_info.name.startswith('_') or module_info.name.endswith('_'):continue
        modules[module_info.name] = f'{pkg.__name__}.{module_info.name}'
    return modules

EPILOG = f"""
        Tips: 
            omit --src to execute all pipelines: {', '.join(get_available_sources().keys())}
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
        choices=get_available_sources().keys(),
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
    print(args)

    # validate

    dst_folder = args.dst_folder

    if dst_folder is not None:
        if not os.path.isabs(dst_folder):
            dst = os.path.abspath(dst_folder)
        if not os.path.exists(dst_folder):
            os.makedirs(dst_folder)

    azure_path = None
    if azure_path is not None:
        if not '/' in azure_path:
           parser.error(f'--azure-path={azure_path} is not a valid Azure path')



    available_sources = get_available_sources()

    pipelines = []
    for src in args.src:
        pipelines.append(available_sources[src])
    storage=get_storage('local')

    for pipeline_fqn in pipelines:

        src_module = importlib.import_module(pipeline_fqn)

        #pipeline = Pipeline(retriever=src_module.Retriever(), transformer=src_module.Transformer())



    # run

    return 0


if __name__ == "__main__":

    configure_logging("INFO")
    raise SystemExit(main(sys.argv))
