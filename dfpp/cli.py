"""
Command line interface for the data pipeline.
"""

import asyncio
import logging
from functools import partial, partialmethod

import click
from dotenv import load_dotenv

from .publishing import publish
from .retrieval import download_indicator_sources
from .transformation import transform_sources
from .utils import list_command

load_dotenv()


def get_logger():
    logging.TRACE = 5
    logging.addLevelName(logging.TRACE, "TRACE")
    logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
    logging.trace = partial(logging.log, logging.TRACE)
    logging.basicConfig()
    azlogger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
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
    logger.name = "dfpp"
    return logger


@click.group()
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(["INFO", "DEBUG", "TRACE"]),
    default="INFO",
    help="Set log level",
)
@click.pass_context
def cli(ctx, **kwargs):
    ctx.obj |= kwargs
    logger = get_logger()
    args = ctx.ensure_object(dict)
    logger.setLevel(args["log_level"])


@cli.command(help="List pipeline info and/or configuration")
@click.option("-i", "--indicators", help="List available indicators", is_flag=True)
@click.option("-s", "--sources", help="List available sources", is_flag=True)
@click.option("-c", "--configs", help="List available configs", is_flag=True)
def list(indicators, sources, configs):
    results = list_command(
        indicators=indicators,
        sources=sources,
        config=configs,
    )
    asyncio.run(results)


@cli.group(chain=True, help="Run specific stages or the whole pipeline")
@click.option(
    "-i",
    "--indicators",
    multiple=True,
    help="Indicator ID(s) to process. If not supplied all detected indicators will be processed.",
)
@click.option(
    "-p",
    "--pattern",
    type=str,
    help="Process only indicators whose IDs contain the pattern.",
)
@click.pass_context
def run(ctx, **kwargs):
    ctx.ensure_object(dict)
    ctx.obj |= kwargs


@run.command(help="Download indicators source raw data")
@click.pass_context
def download(ctx):
    args = ctx.ensure_object(dict)
    results = download_indicator_sources(
        indicator_ids=args["indicators"],
        indicator_id_contain_filter=args["pattern"],
    )
    asyncio.run(results)


@run.command(name="transform", help="Transform/homogenize specific indicators")
@click.pass_context
def transform(ctx):
    args = ctx.ensure_object(dict)
    results = transform_sources(
        indicator_ids=args["indicators"],
        indicator_id_contain_filter=args["pattern"],
        project="access_all_data",
    )
    asyncio.run(results)


@run.command(help="Publish specific indicators to the specified end point")
@click.pass_context
def publish(ctx):
    args = ctx.ensure_object(dict)
    results = publish(
        indicator_ids=args["indicators"],
        indicator_id_contain_filter=args["pattern"],
        project="access_all_data",
    )
    asyncio.run(results)


if __name__ == "__main__":
    cli(obj={})
