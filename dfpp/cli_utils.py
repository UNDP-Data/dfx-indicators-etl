from dfpp.storage import StorageManager


async def list_command(
    indicators=False,
    sources=False,
    config=True,
):
    async with StorageManager() as storage_manager:
        logger.debug("Connected to Azure blob")
        if indicators:
            indicator_files = await storage_manager.list_configs(kind="indicators")
            indicator_ids = [
                os.path.split(sf)[-1].split(".cfg")[0] for sf in indicator_files
            ]
            logger.info(
                f"{len(indicator_ids)} indicators were detected: {json.dumps(indicator_ids, indent=4)}"
            )
        if sources:
            source_files = await storage_manager.list_configs(kind="sources")
            source_ids = [
                os.path.split(sf)[-1].split(".cfg")[0].upper() for sf in source_files
            ]
            logger.info(
                f"{len(source_ids)} indicator sources were detected: {json.dumps(source_ids, indent=4)}"
            )
        if config:
            pipeline_cfg = {"container_name": storage_manager.container_name}
            logger.info(
                f"Pipeline configuration: {json.dumps([pipeline_cfg], indent=4)}"
            )
