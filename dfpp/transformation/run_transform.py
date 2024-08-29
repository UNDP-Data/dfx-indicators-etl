import asyncio
import logging
import os
import papermill as pm
from dfpp.storage import StorageManager
from enum import Enum

__all__ = ["run_notebooks"]

logger = logging.getLogger(__name__)

class NotebookStatus(Enum):
    PROCESSED = "processed"
    MISSING = "missing"
    FAILED = "failed"


@StorageManager.with_storage_manager
async def run_notebooks(
    indicator_ids: list = None,
    indicator_id_contain_filter: str = None,
    storage_manager=None,
):
    """
    Run a list of parameterized notebooks from dfpp.transformation.source_notebooks.
    :param indicator_ids: List of indicator ids to run.
    :param indicator_id_contain_filter: Filter for indicator ids.
    """
    processing_results = {
        "notebooks_processed": set(),
        "notebooks_missing": set(),
        "notebooks_failed": [],
    }

    indicator_cfgs = await storage_manager.get_indicators_cfg(
        indicator_ids=indicator_ids, contain_filter=indicator_id_contain_filter
    )

    logger.debug(f"Retrieved {len(indicator_cfgs)} indicators")

    if not indicator_cfgs:
        logger.info(
            f"No indicators retrieved using indicator_ids={indicator_ids} "
            f"and indicator_id_contain_filter={indicator_id_contain_filter}"
        )
        return processing_results

    unique_source_ids = set([cfg["source_id"] for cfg in indicator_cfgs])

    sources_configs = await storage_manager.get_sources_cfgs(
        source_ids=list(unique_source_ids)
    )

    for source_cfg in sources_configs:
        await execute_notebook(source_cfg, indicator_cfgs, processing_results)

    return processing_results


def log_notebook_result(
    source_id, source_name, notebook_path, status: NotebookStatus, error_message=None
):
    """
    Logs the result of a notebook execution.
    """
    if status == NotebookStatus.PROCESSED:
        logger.info(f"Successfully processed notebook: {notebook_path}")
    elif status == NotebookStatus.FAILED:
        logger.error(
            f"Failed to process notebook for source_id: {source_id}, source_name: {source_name}. "
            f"Notebook: {notebook_path}. Error: {error_message}"
        )
    elif status == NotebookStatus.MISSING:
        logger.warning(f"Notebook missing: {notebook_path}")


def update_processing_results(
    processing_results,
    status: NotebookStatus,
    notebook_path=None,
    source_id=None,
    source_name=None,
    error_message=None,
):
    """
    Updates the processing results dictionary based on the status of the notebook execution.
    """
    if status == NotebookStatus.PROCESSED and notebook_path:
        processing_results["notebooks_processed"].add(notebook_path)
    elif status == NotebookStatus.MISSING and notebook_path:
        processing_results["notebooks_missing"].add(notebook_path)
    elif status == NotebookStatus.FAILED and notebook_path:
        processing_results["notebooks_failed"].append(
            {
                "source_id": source_id,
                "source_name": source_name,
                "notebook_path": notebook_path,
                "error": error_message,
            }
        )


async def execute_notebook(source_cfg, indicator_cfgs, processing_results):
    """
    Executes a single notebook and updates the processing results.
    """
    source_id = source_cfg["id"]
    source_name = source_cfg.get("name", "Unknown Source Name")

    notebook_path = os.path.join(
        "dfpp", "transformation", "source_notebooks", f"{source_id}.ipynb"
    )
    notebook_path_to_save = os.path.join(
        "dfpp", "transformation", "source_notebooks", f"{source_id}_processed.ipynb"
    )

    if not os.path.exists(notebook_path):
        log_notebook_result(
            source_id, source_name, notebook_path, status=NotebookStatus.MISSING
        )
        update_processing_results(
            processing_results, status=NotebookStatus.MISSING, notebook_path=notebook_path
        )
        return

    params = {
        "source_cfg": source_cfg,
        "indicator_cfgs": [
            indicator_cfg
            for indicator_cfg in indicator_cfgs
            if indicator_cfg["source_id"] == source_id
        ],
    }

    try:
        pm.execute_notebook(
            input_path=notebook_path,
            output_path=notebook_path,
            parameters=params,
        )
        log_notebook_result(
            source_id, source_name, notebook_path_to_save, status=NotebookStatus.PROCESSED
        )
        update_processing_results(
            processing_results, status=NotebookStatus.PROCESSED, notebook_path=notebook_path_to_save
        )
    except Exception as e:
        error_message = str(e)
        log_notebook_result(
            source_id,
            source_name,
            notebook_path,
            status=NotebookStatus.FAILED,
            error_message=error_message,
        )
        update_processing_results(
            processing_results,
            status=NotebookStatus.FAILED,
            notebook_path=notebook_path,
            source_id=source_id,
            source_name=source_name,
            error_message=error_message,
        )


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(".env")
    logging.basicConfig()
    logger = logging.getLogger("azure.storage.blob")
    logging_stream_handler = logging.StreamHandler()
    logging_stream_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(logging_stream_handler)
    logger.name = __name__
    asyncio.run(run_notebooks(indicator_ids=None, indicator_id_contain_filter=None))
