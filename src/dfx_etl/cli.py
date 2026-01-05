import argparse
import logging
from typing import List

logger = logging.getLogger(__name__)


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def get_available_pipelines() -> List[str]:
    logger.info("HURI")
    return list("ABC")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dfx-etl",
        description="Run dfx_etl pipelines",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="Tip: omit -p/--pipeline to execute all pipelines.",
    )

    parser.add_argument(
        "-p",
        "--pipeline",
        type=str,
        required=True,
        choices=sorted(get_available_pipelines()),
        help="The name of the pipeline to be executed",
    )

    parser.add_argument(
        "-s",
        "--step",
        type=str,
        required=False,
        choices=["retrieve", "transform", "load", "all"],
        default="all",
        help="The step of the specified pipeline to be executed",
    )

    parser.add_argument(
        "-b",
        "--storage-backend",
        choices=["local", "azure"],
        type=str,
        default="local",
        help="Storage backend where the parquet files will be stored",
    )

    azure_storage = parser.add_argument_group("Azure Storage options")
    azure_storage.add_argument(
        "--container",
        type=str,
        help="The name of the Azure Container where parquet files will be uploaded",
    )

    local_storage = parser.add_argument_group("Local storage options")
    local_storage.add_argument(
        "--local-folder",
        type=str,
        default="./data",
        help="Local output directory/folder where the parquet files will be stored",
    )

    parser.add_argument(
        "-d", "--debug", help="Enable debug logging", action="store_true", default=False
    )

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    if not argv:
        parser.print_help()
        return 1
    args = parser.parse_args(argv)

    # validate



    # run


    return 0


if __name__ == "__main__":

    configure_logging("INFO")
    raise SystemExit(main())
