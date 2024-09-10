"""draft of postgres client to manage indicator insertion"""

import asyncpg


class AsyncPGClient:
    def __init__(self, dsn):
        self.dsn = dsn

    async def connect(self):
        self.conn = await asyncpg.connect(self.dsn)

    async def close(self):
        await self.conn.close()

    async def create_indicator_table(self, indicator_id):
        """
        Create a table for the specific indicator_id if it doesn't exist.
        """
        pass

    async def insert_indicator(self, indicator_id, df_long):
        """
        Insert multiple rows into the indicator_id table from a long format DataFrame.
        """
        pass

    async def read_data(self, indicator_id, country_or_area=None, year=None):
        """
        Read data from the indicator table with optional filters for country and year.
        """
        pass



