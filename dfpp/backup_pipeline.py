import asyncio
import logging
import os

from dfpp.storage import StorageManager

logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)


async def backup_raw_sources():
    try:
        logger.info('Backing up raw sources')
        async with StorageManager() as storage_manager:
            logger.info('Listing raw sources')
            raw_sources_list = await storage_manager.list_blobs(prefix=storage_manager.SOURCES_PATH)
            logger.info(f'Found {len(raw_sources_list)} raw sources')
            # copy the raw sources to the backup folder
            for raw_source_path in raw_sources_list[1:]:
                # print(raw_source_path)
                logger.info(f'Copying {raw_source_path}')
                raw_source_blob_name = raw_source_path.replace(storage_manager.SOURCES_PATH,
                                                               os.path.join(storage_manager.BACKUP_PATH, 'sources'))
                await storage_manager.copy_blob(source_blob_name=raw_source_path,
                                                destination_blob_name=raw_source_blob_name)
                logger.info(f'Copied {raw_source_path} to {raw_source_blob_name}')
    except Exception as e:
        logger.error(f'Error {e} while copying {raw_source_path}')


async def backup_base_files():
    try:
        async with StorageManager() as storage_manager:
            logger.info('Backing up base files')
            base_files_list = await storage_manager.list_blobs(prefix=os.path.join(storage_manager.OUTPUT_PATH, 'access_all_data', 'base'))
            logger.info(f'Found {len(base_files_list)} base files')
            for base_file_path in base_files_list[1:]:
                logger.info(f'Copying {base_file_path}')
                base_file_blob_name = base_file_path.replace(os.path.join(storage_manager.OUTPUT_PATH, 'access_all_data', 'base'), os.path.join(storage_manager.BACKUP_PATH, 'access_all_data', 'base'))
                await storage_manager.copy_blob(source_blob_name=base_file_path, destination_blob_name=base_file_blob_name)
                logger.info(f'Copied {base_file_path} to {base_file_blob_name}')
    except Exception as e:
        logger.error(f'Error {e} while copying {base_file_path}')


async def backup_output_files():
    pass


async def backup_pipeline():
    try:
        await backup_raw_sources()
        await backup_base_files()
        await backup_output_files()
    except Exception as e:
        raise e


if __name__ == "__main__":
    asyncio.run(backup_pipeline())
