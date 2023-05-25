import os
import asyncio
import logging
from dotenv import load_dotenv

from dfpp.storage import AsyncAzureBlobStorageManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
load_dotenv(dotenv_path='../../.env')
ROOT_FOLDER = os.getenv("ROOT_FOLDER")


async def upload_cfgs():
    storage = await AsyncAzureBlobStorageManager.create_instance(
        connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        container_name=os.getenv("CONTAINER_NAME")
    )
    for root, dirs, files in os.walk("output_cfg_files"):
        for file in files:
            if file.endswith(".cfg"):
                logger.info(f"Uploading {file} to Azure Blob Storage")
                await storage.upload(
                    dst_path=os.path.join(ROOT_FOLDER, "config", "sources", root.split('/')[-1], file),
                    src_path=os.path.join(root, file),
                    data=file.encode("utf-8"),
                    content_type="text/plain"
                )
                logger.info(f"Uploaded {file} to Azure Blob Storage")
    await storage.close()


if __name__ == "__main__":
    asyncio.run(upload_cfgs())
