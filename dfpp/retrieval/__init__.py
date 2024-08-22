import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, List


import pandas as pd

from ..storage import StorageManager

from .http import *
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
ASYNC_TASK_TIMEOUT = 720
MAX_DOWNLOAD_CONCURRENCY = 4

logger = logging.getLogger(__name__)

# call_function and any other functions calling it must be in the __init__ to have access to function from
# submodules namespaces


async def call_function(**kwargs) -> Any:
    """
    Asynchronously call a function by name, passing in any arguments specified.

    Parameters:

    - **kwargs: any arguments to pass to the function

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
    function = globals().get(kwargs["downloader_function"])
    if function is not None:
        return await function(**kwargs)
    raise ValueError(f"Function {function} is not defined or not callable")


async def download_for_indicator(
    source_cfg: dict[str, Any],
    storage_manager: StorageManager,
):
    """
    :param source_cfg:
    :param storage_manager:
    :return: number of downloaded/uploaded bytes
    """
    logger.info(
        f"Starting to download source {source_cfg['id']} from {source_cfg['url']} \
              using {source_cfg['downloader_function']}."
    )

    downloader_params = source_cfg["downloader_params"]

    request_params = downloader_params.get("request_params")
    params = {
        "downloader_function": source_cfg["downloader_function"],
        "source_id": source_cfg["id"],
        "source_url": source_cfg["url"],
        "source_save_as": source_cfg.get("save_as"),
        "storage_manager": storage_manager,
        "params_file": downloader_params.get("file"),
    }

    if request_params:
        request_params_dict = json.loads(request_params)
        params.update(
            {
                "params_type": request_params_dict.get("type"),
                "params_url": json.loads(
                    request_params_dict.get("value").replace("'", '"')
                ).get("url"),
                "params_codes": downloader_params.get("codes"),
                "request_params": request_params,
            }
        )

    data, content_type = await call_function(**params)
    # requests_cache.install_cache("cache_name",
    #                              expire_after=3600)  # Cache data for one hour (in seconds)

    logger.info(f"Downloaded {source_cfg['id']} from {source_cfg['url']}.")

    if data is None:
        return 0

    dst_path = os.path.join(storage_manager.sources_path, source_cfg["save_as"])
    await asyncio.create_task(
        storage_manager.upload_blob(
            path_or_data_src=data,
            path_dst=dst_path,
            content_type=content_type,
            overwrite=True,
        )
    )

    return len(data)


async def download_indicator_sources(
    indicator_ids: list[str] | str = None,
    indicator_id_contain_filter: str = None,
) -> List[str]:

    semaphore = asyncio.Semaphore(MAX_DOWNLOAD_CONCURRENCY)

    async with StorageManager() as storage_manager:
        indicator_configs = await storage_manager.get_indicators_cfg(
            indicator_ids=indicator_ids, contain_filter=indicator_id_contain_filter
        )
        unique_source_ids = set([cfg["source_id"] for cfg in indicator_configs])

        sources_cfgs = await storage_manager.get_sources_cfgs(
            source_ids=list(unique_source_ids)
        )
        source_indicator_map = build_source_indicator_map(
            indicator_configs, sources_cfgs
        )

        logger.info(
            f"Detected {len(unique_source_ids)} sources for \
                {len(indicator_configs)} indicators"
        )

        download_tasks = [
            create_download_task(source_cfg, storage_manager, semaphore)
            for source_cfg in sources_cfgs
            if source_cfg["source_type"] != "Manual"
        ]

        await process_tasks(download_tasks, source_indicator_map)

    return finalize_report(source_indicator_map)


def build_source_indicator_map(indicator_configs, source_configs):

    source_indicator_map = {}

    for source_cfg in source_configs:
        source_id = source_cfg["id"]
        if source_id not in source_indicator_map:
            source_indicator_map[source_id] = {
                "indicators": [],
                "downloaded": False,
            }
        source_indicator_map[source_id].update(**source_cfg)

    for indicator_cfg in indicator_configs:
        source_id = indicator_cfg["source_id"]
        source_indicator_map[source_id]["indicators"].append(
            indicator_cfg["indicator_id"]
        )

    return source_indicator_map


def create_download_task(source_cfg, storage_manager, semaphore):
    async def task_wrapper(semaphore):
        async with semaphore:
            return await asyncio.wait_for(
                download_for_indicator(source_cfg, storage_manager),
                timeout=ASYNC_TASK_TIMEOUT,
            )

    return asyncio.create_task(task_wrapper(semaphore), name=source_cfg["id"])


async def process_tasks(tasks, source_indicator_map):
    done, _ = await asyncio.wait(tasks)

    for task in done:
        try:
            source_id = task.get_name()
            data_size_bytes = await task
            source_indicator_map[source_id]["downloaded"] = data_size_bytes >= 100
            if not source_indicator_map[source_id]["downloaded"]:
                raise ValueError("File size is below 100 bytes")
        except (asyncio.TimeoutError, TimeoutError):
            log_task_timeout(source_id, source_indicator_map)
        except Exception as e:
            log_task_exception(source_id, e, source_indicator_map)


def log_task_exception(source_id, exception, source_indicator_map):
    error_message = f"Error processing {source_id}: {exception}"
    logger.error(error_message)
    source_indicator_map[source_id]["downloaded"] = False
    source_indicator_map[source_id]["error"] = error_message


def log_task_timeout(source_id, source_indicator_map):
    error_message = f"Async task timeout {ASYNC_TASK_TIMEOUT} seconds \
        reached while downloading source {source_id}"
    logger.error(error_message)
    source_indicator_map[source_id]["downloaded"] = False
    source_indicator_map[source_id]["error"] = error_message


def finalize_report(source_indicator_map):
    log_report_summary(source_indicator_map)
    generate_error_report(source_indicator_map)


def log_report_summary(source_indicator_map):
    total_sources = len(source_indicator_map)
    downloaded_sources = len(
        [
            source_id
            for source_id, details in source_indicator_map.items()
            if details["downloaded"] == True
        ]
    )
    downloaded_indicators = sum(
        [
            len(details["indicators"])
            for source_id, details in source_indicator_map.items()
            if details["downloaded"] == True
        ]
    )
    failed_sources = [
        source_id
        for source_id, details in source_indicator_map.items()
        if not details["downloaded"] and details["source_type"] != "Manual"
    ]

    skipped_sources = [
        source_id
        for source_id, details in source_indicator_map.items()
        if details["source_type"] == "Manual"
    ]

    logger.info("#" * 200)
    logger.info(f"TASKED: {total_sources} sources")

    logger.info(
        f"DOWNLOADED: {downloaded_sources} sources for {downloaded_indicators} indicators"
    )
    logger.info(f"FAILED: {len(failed_sources)} sources")
    logger.info(f"SKIPPED: {len(skipped_sources)} sources")
    logger.info("#" * 200)


def generate_error_report(source_indicator_map):
    failed_sources = [
        (source_id, details)
        for source_id, details in source_indicator_map.items()
        if not details["downloaded"]
    ]
    if not failed_sources:
        return

    error_reports = []
    for source_id, details in failed_sources:
        if details["source_type"] == "Manual":
            continue

        flat_report = {
            "source_id": source_id,
            "indicators": json.dumps(details["indicators"]),
            "downloaded": details["downloaded"],
            "error": details.get("error"),
        }

        for key, value in details.items():
            if key not in {"indicators", "downloaded", "error"}:
                flat_report[key] = (
                    json.dumps(value) if isinstance(value, (dict, list)) else value
                )

        error_reports.append(flat_report)

    df_errors = pd.DataFrame(error_reports)
    df_errors.to_csv(
        f"download_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mode="a" if os.path.exists("download_errors.csv") else "w",
        index=False,
    )
