"""publish transfromed series data into a blob storage"""

import os
from io import BytesIO
import pandas as pd

from dfpp.storage import StorageManager

__all__ = ["publish_series"]


@StorageManager.with_storage_manager
async def publish_series(
    series_id: str, df_series: pd.DataFrame, source_folder: str = None, storage_manager: StorageManager = None
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
            storage_manager.test_path, source_folder, "xlsx", f"{series_id}.xlsx"
        )
        blob_client = storage_manager.container_client.get_blob_client(
            blob=path_to_save
        )

        await blob_client.upload_blob(data=output_buffer.getvalue(), overwrite=True)

    with BytesIO() as output_buffer_json:
        json_data = df_series.to_json(orient='records')
        output_buffer_json.write(json_data.encode())
        output_buffer_json.seek(0)

    
        path_to_save_json = os.path.join(
            storage_manager.test_path, source_folder, "json", f"{series_id}.json"
        )
        
        blob_client_json = storage_manager.container_client.get_blob_client(
            blob=path_to_save_json
        )
        await blob_client_json.upload_blob(data=output_buffer_json.getvalue(), overwrite=True)

