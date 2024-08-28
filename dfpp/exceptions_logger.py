import logging
logger = logging.getLogger(__name__)

def handle_exceptions(func, logger=logger):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            raise e
    return wrapper