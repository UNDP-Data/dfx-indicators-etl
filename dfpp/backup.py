import asyncio
import logging
import os

from .storage import StorageManager

logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)


async def backup_raw_sources(indicator_ids: list = None):
    try:
        logger.info("Backing up raw sources")
        async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER"),
        ) as storage_manager:
            if indicator_ids and len(indicator_ids) > 0:
                source_ids = list(
                    set(
                        [
                            indicator_cfg.get("indicator").get("source_id")
                            for indicator_cfg in await storage_manager.get_indicators_cfg(
                                indicator_ids=indicator_ids
                            )
                        ]
                    )
                )
            else:
                source_ids = list(
                    set(
                        [
                            indicator_cfg.get("indicator").get("source_id")
                            for indicator_cfg in await storage_manager.get_indicators_cfg()
                        ]
                    )
                )
                save_as = [
                    source_cfg.get("source").get("save_as")
                    for source_cfg in await storage_manager.get_sources_cfgs(
                        source_ids=source_ids
                    )
                ]
            raw_sources_list = [
                f"{storage_manager.SOURCES_PATH}/{save_as[i]}"
                for i, source_id in enumerate(source_ids)
            ]
            logger.info(f"Found {len(raw_sources_list)} raw sources")
            for raw_source_path in raw_sources_list:
                logger.info(f"Backing up {raw_source_path}")
                raw_backup_source_blob_name = raw_source_path.replace(
                    storage_manager.SOURCES_PATH,
                    os.path.join(storage_manager.BACKUP_PATH, "sources"),
                )
                await storage_manager.copy_blob(
                    source_blob_name=raw_source_path,
                    destination_blob_name=raw_backup_source_blob_name,
                )
                logger.info(
                    f"Created a backup for {raw_source_path} to {raw_backup_source_blob_name}"
                )
    except Exception as e:
        logger.error(f"Error {e} while copying file")


async def backup_base_files(indicator_ids: list = None):
    try:
        logger.info("Backing up base files")

        async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER"),
        ) as storage_manager:

            # Function to fetch source_ids and base file paths
            async def get_base_files_list(indicator_ids=None):
                indicator_cfgs = await storage_manager.get_indicators_cfg(
                    indicator_ids=indicator_ids
                )
                source_ids = list(
                    set(cfg.get("indicator").get("source_id") for cfg in indicator_cfgs)
                )
                base_files = [
                    f"{storage_manager.OUTPUT_PATH}/access_all_data/base/{source_id}.csv"
                    for source_id in source_ids
                ]
                return base_files

            base_files_list = await get_base_files_list(indicator_ids)

            logger.info(f"Found {len(base_files_list)} base files")

            for base_file_path in base_files_list:
                try:
                    logger.info(f"Backing up {base_file_path}")
                    base_backup_file_blob_name = base_file_path.replace(
                        storage_manager.OUTPUT_PATH, storage_manager.BACKUP_PATH
                    )

                    await storage_manager.copy_blob(
                        source_blob_name=base_file_path,
                        destination_blob_name=base_backup_file_blob_name,
                    )

                    logger.info(
                        f"Created a backup for {base_file_path} to {base_backup_file_blob_name}"
                    )

                except Exception as e:
                    logger.error(f"Error {e} while copying {base_file_path}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")


async def backup_pipeline(indicator_ids: list = None):
    try:
        # await backup_raw_sources(indicator_ids=indicator_ids)
        await backup_base_files(indicator_ids=indicator_ids)
    except Exception as e:
        raise e


if __name__ == "__main__":
    asyncio.run(backup_pipeline())
