import logging

def handle_exceptions(logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    def decorator(func):
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {e}")
                raise
        return wrapper
    return decorator
