import asyncio

from dotenv import load_dotenv

load_dotenv()
import logging

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
from .storage import StorageManager


async def run():
    async with StorageManager() as sm:

        logger.info(f'connected to azure')
        i = await sm.get_indicators_cfg(indicator_ids=[], contain_filter=None)
        print(i)

if __name__ == '__main__':
    asyncio.run(run())