import asyncio
import logging
import os
import pathlib

from dotenv import load_dotenv

from dfpp.storage import StorageManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()
ROOT_FOLDER = os.getenv("ROOT_FOLDER")


async def upload_source_cfgs(source_cfgs_path: pathlib.Path) -> None:
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        for root, dirs, files in os.walk(source_cfgs_path):
            for file in files:
                if file.endswith(".cfg"):
                    logger.info(f"Uploading {file} to Azure Blob Storage")
                    await storage_manager.upload(
                        dst_path=os.path.join(ROOT_FOLDER, "config", "sources", root.split('/')[-1], file),
                        src_path=os.path.join(root, file),
                        data=file.encode("utf-8"),
                        content_type="text/plain"
                    )
                    logger.info(f"Uploaded {file} to Azure Blob Storage")


async def upload_indicator_cfgs(indicator_cfgs_path: pathlib.Path) -> None:
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        for root, dirs, files in os.walk(indicator_cfgs_path):
            for file in files:
                if file.endswith(".cfg"):
                    logger.info(f"Uploading {file} to Azure Blob Storage")
                    await storage_manager.upload(
                        dst_path=os.path.join(ROOT_FOLDER, "config", "indicators", file),
                        src_path=os.path.join(root, file),
                        data=file.encode("utf-8"),
                        content_type="text/plain"
                    )
                    logger.info(f"Uploaded {file} to Azure Blob Storage")


if __name__ == "__main__":
    # asyncio.run(upload_source_cfgs(
    #     source_cfgs_path=pathlib.Path("/home/thuha/Desktop/UNDP/dfp/dv-data-pipeline/configs/sources/sources")))
    asyncio.run(upload_indicator_cfgs(
        indicator_cfgs_path=pathlib.Path("/home/thuha/Desktop/UNDP/dfp/dv-data-pipeline/configs/indicators")))
