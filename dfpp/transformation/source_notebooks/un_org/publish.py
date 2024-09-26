"""publish transfromed series data into a blob storage"""

import os
from io import BytesIO
import pandas as pd
import json

from dfpp.storage import StorageManager

__all__ = ["publish_series"]


@StorageManager.with_storage_manager
async def publish_series(
    series_id: str, df_series: pd.DataFrame, storage_manager: StorageManager
) -> None:
    """
    Publish the transformed series data into a blob storage.

    Args:
        series_id (str): The identifier of the series.
        df_series (SeriesDataFrame): The transformed series data.
        storage_manager (StorageManager): The storage manager object for accessing Azure Blob Storage.
    Returns:
        None
    """
    with BytesIO() as output_buffer:
        df_series.to_excel(output_buffer, index=False, engine="openpyxl")
        output_buffer.seek(0)

        path_to_save = os.path.join(
            storage_manager.test_path, "unstats_un_org", f"{series_id}.xlsx"
        )
        blob_client = storage_manager.container_client.get_blob_client(
            blob=path_to_save
        )

        await blob_client.upload_blob(data=output_buffer.getvalue(), overwrite=True)
