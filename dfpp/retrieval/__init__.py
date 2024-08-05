import asyncio
import json
import logging
import os
from io import StringIO
from traceback import print_exc
from typing import Any

import aiohttp
import pandas as pd

from ..storage import StorageManager
from ..utils import chunker
from .http import *
from .http import simple_url_get
from .sipri import *
from .utils import *
from .vdem import *
from .world_bank import *

__all__ = [
    "country_downloader",
    "call_function",
    "download_for_indicator",
    "download_indicator_sources",
]

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(
    total=120, connect=20, sock_connect=20, sock_read=20
)

logger = logging.getLogger(__name__)

# call_function and any other functions calling it must be in the __init__ to have access to function from
# submodules namespaces


async def call_function(function_name, *args, **kwargs) -> Any:
    """
    Asynchronously call a function by name, passing in any arguments specified.

    Parameters:
    - function_name (str): the name of the function to call
    - *args: any arguments to pass to the function

    Returns:
    - the result of the function call, or None if the function name is None

    Example usage:
    ```
    async def my_function(arg1, arg2):
        # do some work
        return result

    result = await call_function('my_function', arg1, arg2)
    ```
    """
    function = globals().get(function_name)
    assert function is not None, f"Function {function_name} is not defined"
    if function is not None:
        return await function(*args, **kwargs)
    else:
        raise ValueError(f"Function {function} is not defined or not callable")


async def download_for_indicator(
    indicator_cfg: dict[str, Any],
    source_cfg: dict[str, Any],
    storage_manager: StorageManager,
    sync_upload=True,
):
    """

    :param indicator_cfg:
    :param source_cfg:
    :param storage_manager:
    :param sync_upload: default=False, use sync blob client to upload
    :return: number of downloaded/uploaded bytes
    """
    source_id = indicator_cfg["indicator"]["source_id"]
    assert source_id is not None, "Source ID must be specified in indicator config"
    # try:
    logger.info(
        f"Starting to download source {source_id} from {source_cfg['source']['url']} using {source_cfg['source']['downloader_function']}."
    )

    downloader_params = source_cfg["downloader_params"]
    # requests_cache.install_cache("cache_name",
    #                              expire_after=3600)  # Cache data for one hour (in seconds)
    if downloader_params.get("request_params") is None:
        data, content_type = await call_function(
            source_cfg["source"]["downloader_function"],
            source_id=source_id,
            source_url=source_cfg["source"].get("url"),
            source_save_as=source_cfg["source"].get("save_as"),
            storage_manager=storage_manager,
            params_file=downloader_params.get("file"),
        )
    else:
        request_params = json.loads(downloader_params.get("request_params"))
        params_file = downloader_params.get("file")
        params_type = (request_params.get("type"),)
        params_url = (
            json.loads(request_params.get("value").replace("'", '"')).get("url"),
        )
        params_codes = (downloader_params.get("codes"),)
        data, content_type = await call_function(
            source_cfg["source"]["downloader_function"],
            source_id=source_id,
            source_url=source_cfg["source"].get("url"),
            source_save_as=source_cfg["source"].get("save_as"),
            params_type=params_type,
            params_url=params_url,
            params_codes=params_codes,
            params_file=params_file,
            request_params=downloader_params.get("request_params"),
            storage_manager=storage_manager,
        )
    logger.info(f"Downloaded {source_id} from {source_cfg['source']['url']}.")
    # it makes sense to combine the download and upload here because  an indicatpr has been downloaded
    # if the source data have been downloaded and the result uploaded to azure
    if data is not None:

        dst_path = os.path.join(
            storage_manager.SOURCES_PATH, source_cfg["source"]["save_as"]
        )
        if sync_upload is False:
            await asyncio.create_task(
                storage_manager.upload(
                    data=data,
                    content_type=content_type,
                    dst_path=dst_path,
                    overwrite=True,
                )
            )
        else:
            await storage_manager.upload(
                data=data, content_type=content_type, dst_path=dst_path, overwrite=True
            )

        return len(data)
    else:
        return 0


async def download_indicator_sources(
    indicator_ids: list[str] | str = None,
    indicator_id_contain_filter: str = None,
    concurrent_chunk_size: int = 50,
) -> list[str]:
    failed_source_ids = []
    skipped_source_ids = []
    source_indicator_map = {}
    source_indicator_map_tod = {}
    error_reports = []

    async with StorageManager() as storage_manager:
        logger.debug(f"Connected to Azure blob")

        indicator_configs = await storage_manager.get_indicators_cfg(
            indicator_ids=indicator_ids, contain_filter=indicator_id_contain_filter
        )
        sources = [
            indicator_cfg["indicator"]["source_id"]
            for indicator_cfg in indicator_configs
        ]
        unique_source_ids = set(sources)
        for c in indicator_configs:
            src = c["indicator"]["source_id"]
            ind = c["indicator"]["indicator_id"]
            if not src in source_indicator_map_tod:
                source_indicator_map_tod[src] = [ind]
            else:
                source_indicator_map_tod[src].append(ind)

        logger.info(
            f" {len(unique_source_ids)} sources defining {len(indicator_configs)} indicators have been detected in the config folder {storage_manager.INDICATORS_CFG_PATH}"
        )

        for chunk in chunker(unique_source_ids, size=concurrent_chunk_size):
            download_tasks = []
            for source_id in chunk:

                indicator_cfg = list(
                    filter(
                        lambda x: x["indicator"]["source_id"] == source_id,
                        indicator_configs,
                    )
                )[0]

                source_id = indicator_cfg["indicator"].get("source_id")
                # get source config is checking for existence as well
                try:
                    source_cfg = await storage_manager.get_source_cfg(
                        source_id=source_id
                    )
                    if source_cfg["source"]["source_type"] == "Manual":
                        logger.info(f"Skipping manual source {source_id}")
                        # skipped_source_ids.append(source_id)
                        source_indicator_map[source_id] = source_indicator_map_tod[
                            source_id
                        ]
                        continue
                    if source_cfg["source"].get("save_as") is None:  # compute missing
                        save_as = f"{source_id}.{source_cfg['url'].split('.')[-1]}"
                        logger.warning(
                            f"Source data for {source_id} wil be saved  to  {save_as}"
                        )
                        source_cfg["source"]["save_as"] = save_as
                    download_task = asyncio.create_task(
                        download_for_indicator(
                            indicator_cfg=indicator_cfg,
                            source_cfg=source_cfg,
                            storage_manager=storage_manager,
                        ),
                        name=source_id,
                    )

                    download_tasks.append(download_task)
                except Exception as e:
                    logger.error(f"Failed to download/upload source {source_id} ")
                    logger.error(e)
                    error_reports.append(
                        {
                            "indicator_id": indicator_cfg["indicator"]["indicator_id"],
                            "source_id": source_id,
                            "error": e,
                        }
                    )
                    failed_source_ids.append(source_id)
                    continue
            if download_tasks:
                logger.info(
                    f"Downloading {len(download_tasks)} indicator sources concurrently"
                )
                done, pending = await asyncio.wait(
                    download_tasks,
                    return_when=asyncio.ALL_COMPLETED,
                    timeout=concurrent_chunk_size * DEFAULT_TIMEOUT.total + 10,
                    # to make 100% sure the download
                    # never gets stuck
                )
                if done:
                    logger.info(f"Collecting results for {len(chunk)} sources")
                    for done_task in done:

                        try:
                            source_id = done_task.get_name()
                            data_size_bytes = await done_task
                            if (
                                data_size_bytes < 100
                            ):  # TODO: establish  a realistic value
                                logger.warning(
                                    f"No data was downloaded for indicator {source_id}"
                                )
                                failed_source_ids.append(source_id)
                            else:
                                source_indicator_map[source_id] = (
                                    source_indicator_map_tod[source_id]
                                )
                        except Exception as e:
                            failed_source_ids.append(source_id)
                            with StringIO() as m:
                                print_exc(file=m)
                                em = m.getvalue()
                                logger.error(
                                    f"Error {em} was encountered while processing  {source_id}"
                                )

                if pending:
                    logger.debug(
                        f"{len(pending)} out of {len(chunk)} sources  have timed out"
                    )

                    for pending_task in pending:

                        try:
                            source_id, indicator_id = done_task.get_name().split("::")
                            pending_task.cancel()
                            await pending_task
                            failed_source_ids.append(source_id)
                            error_reports.append(
                                {
                                    "indicator_id": indicator_id,
                                    "source_id": source_id,
                                    "error": "Timeout",
                                }
                            )
                        except asyncio.CancelledError:
                            logger.debug(
                                f"Pending future for source {source_id} has been cancelled"
                            )
                        except Exception as e:
                            error_reports.append(
                                {
                                    "indicator_id": indicator_id,
                                    "source_id": source_id,
                                    "error": e,
                                }
                            )
                            raise e
            else:
                logger.info(f"No sources were downloaded")

    downloaded_indicators = sorted(
        [item for sublist in source_indicator_map.values() for item in sublist]
    )

    logger.info("#" * 200)
    logger.info(
        f"TASKED: {len(unique_source_ids)} sources defining {len(indicator_configs)} indicators"
    )
    logger.info(
        f"DOWNLOADED:  {len(source_indicator_map.keys())} sources defining {len(downloaded_indicators)} indicators"
    )
    if failed_source_ids:
        failed_indicators = []
        for fsource in failed_source_ids:
            failed_indicators += source_indicator_map_tod[fsource]
        logger.info(
            f"FAILED {len(failed_source_ids)} defining {len(failed_indicators)} indicators"
        )
    if skipped_source_ids:
        skipped_indicators = []
        for ssource in skipped_indicators:
            skipped_indicators += source_indicator_map_tod[ssource]
        logger.info(
            f"SKIPPED {len(skipped_source_ids)} defining {len(skipped_indicators)} indicators"
        )
    logger.info("#" * 200)

    df_errors = pd.DataFrame(error_reports)
    if not df_errors.empty:
        file_path = "error_report.csv"
        mode = "a" if os.path.exists(file_path) else "w"
        df_errors.to_csv(file_path, mode=mode, index=False)
    return downloaded_indicators
