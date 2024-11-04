import logging
import os
from datetime import datetime
from enum import Enum

import pandas as pd
import papermill as pm

from dfpp.common import get_domain_from_url, get_netloc_from_url, snake_casify
from dfpp.storage import StorageManager

__all__ = ["run_notebooks"]

logger = logging.getLogger(__name__)

BASE_NOTEBOOK_PATH = os.path.join("dfpp", "transformation", "source_notebooks")


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
        "notebooks_processed": [],
        "notebooks_missing": [],
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

    processing_results = pd.DataFrame(
        processing_results["notebooks_processed"]
        + processing_results["notebooks_missing"]
        + processing_results["notebooks_failed"]
    )

    output_summary(processing_results)
    save_results_to_excel(processing_results)

    return processing_results


def log_and_update_results(
    processing_results,
    status: NotebookStatus,
    notebook_path=None,
    source_id=None,
    indicator_id=None,
    source_name=None,
    error_message=None,
):
    """
    Wrapper function to log notebook results and update processing results.
    """
    log_notebook_result(
        source_id,
        source_name,
        notebook_path,
        status=status,
        indicator_id=indicator_id,
        error_message=error_message,
    )
    update_processing_results(
        processing_results,
        status=status,
        notebook_path=notebook_path,
        source_id=source_id,
        indicator_id=indicator_id,
        source_name=source_name,
        error_message=error_message,
    )


def log_notebook_result(
    source_id,
    source_name,
    notebook_path,
    status: NotebookStatus,
    indicator_id=None,
    error_message=None,
):
    """
    Logs the result of a notebook execution.
    """
    if status == NotebookStatus.PROCESSED:
        logger.info(
            f"Successfully processed notebook: {notebook_path} for indicator_id: {indicator_id}"
        )
    elif status == NotebookStatus.FAILED:
        logger.error(
            f"Failed to process notebook for source_id: {source_id}, source_name: {source_name}, indicator_id: {indicator_id}. "
            f"Notebook: {notebook_path}. Error: {error_message}"
        )
    elif status == NotebookStatus.MISSING:
        logger.warning(
            f"Notebook missing: {notebook_path} for indicator_id: {indicator_id}"
        )


def update_processing_results(
    processing_results,
    status: NotebookStatus,
    notebook_path=None,
    source_id=None,
    indicator_id=None,
    source_name=None,
    error_message=None,
):
    """
    Updates the processing results dictionary based on the status of the notebook execution.
    """
    result = {
        "source_id": source_id,
        "indicator_id": indicator_id,
        "source_name": source_name,
        "notebook_path": notebook_path,
        "status": status.value,
        "error": error_message,
    }

    if status == NotebookStatus.PROCESSED:
        processing_results["notebooks_processed"].append(result)
    elif status == NotebookStatus.MISSING:
        processing_results["notebooks_missing"].append(result)
    elif status == NotebookStatus.FAILED:
        processing_results["notebooks_failed"].append(result)


async def execute_notebook(source_cfg, indicator_cfgs, processing_results):
    """
    Executes a SOURCE notebook for ALL associated indicators
    """
    source_id = source_cfg["id"]
    source_name = source_cfg.get("name", "Unknown Source Name")

    notebook_path = os.path.join(BASE_NOTEBOOK_PATH, f"{source_id}.ipynb")
    if source_cfg["source_type"] != "Manual":
        domain = snake_casify(get_domain_from_url(source_cfg["url"]))
        netloc = snake_casify(get_netloc_from_url(source_cfg["url"]))

        notebook_path = os.path.join(BASE_NOTEBOOK_PATH, domain, f"{netloc}.ipynb")

    if not os.path.exists(notebook_path):
        log_and_update_results(
            processing_results,
            status=NotebookStatus.MISSING,
            notebook_path=notebook_path,
            source_id=source_id,
            source_name=source_name,
        )
        return None

    source_indicator_cfgs = [
        indicator_cfg
        for indicator_cfg in indicator_cfgs
        if indicator_cfg["source_id"] == source_id
    ]
    path_to_save_executed_notebook = os.path.join(
        BASE_NOTEBOOK_PATH, domain, "indicator_execution"
    )
    if not os.path.exists(path_to_save_executed_notebook):
        os.mkdir(path_to_save_executed_notebook)

    for indicator_cfg in source_indicator_cfgs:
        indicator_id = indicator_cfg["indicator_id"]

        params = {"source_cfg": source_cfg, "indicator_cfg": indicator_cfg}

        notebook_path_to_save = os.path.join(
            path_to_save_executed_notebook, f"{source_id}_{indicator_id}.ipynb"
        )

        try:
            pm.execute_notebook(
                input_path=notebook_path,
                output_path=notebook_path_to_save,
                parameters=params,
            )
            log_and_update_results(
                processing_results,
                status=NotebookStatus.PROCESSED,
                notebook_path=notebook_path_to_save,
                indicator_id=indicator_id,
                source_id=source_id,
                source_name=source_name,
            )
        except Exception as e:
            error_message = str(e)
            log_and_update_results(
                processing_results,
                status=NotebookStatus.FAILED,
                notebook_path=notebook_path_to_save,
                source_id=source_id,
                source_name=source_name,
                error_message=error_message,
                indicator_id=indicator_id,
            )


def save_results_to_excel(processing_results):
    """
    Saves the processing results to an Excel file, appending if the file already exists.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processing_results["timestamp"] = timestamp
    filename = f"processing_results_{timestamp}.xlsx"

    with pd.ExcelWriter(filename) as writer:
        processing_results.to_excel(writer, index=False, sheet_name=timestamp)


def output_summary(processing_results_df):
    """
    Output summary statistics of the processing results.
    """
    num_sources = processing_results_df.source_id.nunique()

    num_missing_source_notebooks = (
        processing_results_df["status"].eq(NotebookStatus.MISSING.value).sum()
    )

    num_failed = processing_results_df["status"].eq(NotebookStatus.FAILED.value).sum()

    sources_processed = processing_results_df[
        processing_results_df["status"] == NotebookStatus.PROCESSED.value
    ]["source_id"].nunique()

    indicators_processed = processing_results_df[
        processing_results_df["status"] == NotebookStatus.PROCESSED.value
    ]["indicator_id"].nunique()

    logger.info(f"Total Sources: {num_sources}")
    logger.info(f"Total Notebooks Missing: {num_missing_source_notebooks}")
    logger.info(f"Total Notebooks Failed: {num_failed}")
    logger.info(f"Number of Sources Processed: {sources_processed}")
    logger.info(f"Number of Indicators Successfully Processed: {indicators_processed}")
