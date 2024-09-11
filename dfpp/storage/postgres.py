"""mock of postgres client to manage indicator insertion"""

import asyncpg

class AsyncPGClient:
    def __init__(self):
        self.tables = {}  
        self.connected = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self):
        self.connected = True
        print("Mock connected to the database.")

    async def close(self):
        self.connected = False
        print("Mock closed the database connection.")

    async def create_indicator_table(self, indicator_id):
        if indicator_id not in self.tables:
            self.tables[indicator_id] = []
            print(f"Created mock table for indicator: {indicator_id}")
        else:
            print(f"Table for indicator {indicator_id} already exists.")

    async def insert_indicator(self, indicator_id, df_long):

        print(f"Inserted {df_long.shape[0]} rows into table for indicator: {indicator_id}")

    async def read_data(self, indicator_id, country_or_area=None, year=None):
        if indicator_id not in self.tables:
            raise Exception(f"Table for indicator {indicator_id} does not exist.")

        print(f"Reading data from table {indicator_id}.")
        return data