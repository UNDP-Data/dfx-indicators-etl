import json
import logging
import os

import asyncpg
import pandas
import pandas as pd

from dfpp import constants

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

logger = logging.getLogger(__name__)

ALLOWED_METHODS = 'execute', 'fetch', 'fetchval', 'fetchrow'


async def run_query(conn_obj=None, sql_query=None, method='execute'):
    """
    Run a SQL query represented by sql_query against the context represented by conn_obj
    :param conn_obj: asyncpg connection object
    :param sql_query: str, SQL query
    :param method: str, the asyncpg method used to perform the query. One of
    'execute', 'fetch', 'fetch_val', 'fetch_all'
    :return:
    """
    assert conn_obj is not None, f'invalid conn_obj={conn_obj}'
    assert sql_query not in ('', None), f'Invalid sql_query={sql_query}'
    assert method not in ('', None), f'Invalid method={method}'
    assert method in ALLOWED_METHODS, f'Invalid method={method}. Valid option are {",".join(ALLOWED_METHODS)}'
    m = getattr(conn_obj, method)
    #async with conn_obj.transaction():
    return await m(sql_query)


async def table_exists(conn_obj=None, table=None):
    """
    Check if a given table exists
    :param conn_obj: instance of asyncpg.connection

    :param table_name: str, the name of the table
    :return: True if the table exists false otherwise
    """
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    sql_query = f'SELECT * from {table} LIMIT 0;'
    try:
        await run_query(
            conn_obj=conn_obj,
            sql_query=sql_query,
            method='execute'
        )
        return True

    except asyncpg.exceptions.UndefinedTableError:
        return False


async def drop_table(conn_obj=None, table=None):
    """
    Removes a given table from the database encapsulated into the conn_obj
    :param conn_obj: instance of asyncpg_connection
    :param sql_file_name: str, the name of the SQL template script
    :param table: str, fully qualified name of the table to be removed
    :return: None
    """
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    sql_query = f'DROP TABLE IF EXISTS {table};'
    await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='execute'
    )


async def create_out_table(conn_obj=None, table=None):
    """
    Create main output table
    :param conn_obj: instance of asyncpg.connection
    :param table: str, the fully qualified table name of the table
    :return: None
    """
    assert table not in ('', None), f'Invalid table={table}'

    assert '.' in table, f'table={table} is not fully qualified'
    sql_query = \
        f'''
        
            CREATE TABLE {table} (
                indicator_id text,
                country_iso3 varchar(3) NOT NULL,
                year smallint NOT NULL,
                value numeric(100, 2),
                PRIMARY KEY (indicator_id, country_iso3, year)
            );
            
            CREATE INDEX year_idx ON {table} USING brin(year);
            CREATE INDEX country_idx ON {table} USING btree(country_iso3);
            
            
        '''
    try:
        await run_query(
            conn_obj=conn_obj,
            sql_query=sql_query,
            method='execute'
        )
        return True

    except asyncpg.exceptions.UndefinedTableError:
        return False


async def quiet_upsert(conn_obj=None, table=None, df: pandas.DataFrame = None, overwrite=False):
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    col_names_str = json.dumps(tuple(df.columns.to_list()))

    if overwrite and await table_exists(conn_obj=conn_obj, table=table):
        await drop_table(conn_obj=conn_obj, table=table)
        await create_out_table(conn_obj=conn_obj, table=table)

    sql_query = \
        f'''
                INSERT INTO {table} ({col_names_str[1:-1]})
                VALUES (
                    $1, $2, $3, $4
                )
                ON CONFLICT ON CONSTRAINT dfpp_pkey DO UPDATE
                SET 
                    value = EXCLUDED.value
                WHERE 
                    {table}.value <> EXCLUDED.value;
            '''
    await conn_obj.executemany(sql_query, df.values.tolist())


async def upsert(conn_obj=None, table=None, df: pandas.DataFrame = None, overwrite=False):
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    col_names_str = json.dumps(tuple(df.columns.to_list()))


    overwrite_clause = f'WHERE {table}.value <> EXCLUDED.value' if overwrite is False else ''

    sql_queryf = \
        f'''
                INSERT INTO {table} ({col_names_str[1:-1]}) (SELECT * FROM unnest($1::{table}[])
                                                            )
                ON CONFLICT ON CONSTRAINT dfpp_pkey DO UPDATE
                SET 
                    value = EXCLUDED.value
                {overwrite_clause}
                RETURNING indicator_id, xmax = 0 as inserted, xmax = xmin as updated;


            '''
    results = await conn_obj.fetch(sql_queryf, [tuple(e) for e in df.values])
    if results:
        riter = ({'indicator_id': e['indicator_id'], 'inserted': e['inserted'], 'updated': e['updated']} for e in results)
        res_df = pd.DataFrame.from_dict(riter)
        inserted = res_df.inserted.sum()
        updated = res_df.updated.sum()
        skipped = abs(len(df)-(updated+inserted))
        logger.info(f'Total rows: {len(df)} of which inserted {inserted} and updated {updated} skipped {skipped}')
    else:
        logger.info('No records were updated/inserted because no records were found to be different ')


async def run(dsn=None, table='staging.dfpp', df=None, overwrite=False):

    async with asyncpg.create_pool(dsn=dsn, min_size=constants.POOL_MINSIZE, max_size=constants.POOL_MAXSIZE,
                                   command_timeout=constants.POOL_COMMAND_TIMEOUT, ) as pool:
        logger.debug('Connecting to database...')
        async with pool.acquire(timeout=constants.CONNECTION_TIMEOUT) as conn_obj:
            if not await table_exists(conn_obj=conn_obj, table=table):
                # await drop_table(conn_obj=conn_obj, table=table)
                await create_out_table(conn_obj=conn_obj, table=table)
            await upsert(conn_obj=conn_obj, table=table, df=df, overwrite=overwrite)


if __name__ == '__main__':
    import asyncio

    dsn = os.environ.get('POSTGRES_DSN')
    logging.basicConfig()
    # azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
    # azlogger.setLevel(logging.WARNING)
    logger = logging.getLogger()
    logging_stream_handler = logging.StreamHandler()
    logging_stream_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(logging_stream_handler)
    logger.name = 'dfpp'
    col_names  = ['indicator_id', 'country_iso3', 'year', 'value']
    df = pd.DataFrame(
        columns=col_names,
        data=[
            ('atestindi', 'AZE', 1995,  22.5678),
            ('atestindi', 'AZE', 1996, 27),
            ('atestindi', 'AZE', 1997,  29.8),
            ('atestindi', 'AZE', 1998,  24.9),
            ('atestindi', 'AZE', 1999,  25.5),
            ('atestindi', 'AZE', 2000, None),
        ]
    )
    asyncio.run(run(dsn=dsn, df=df,  overwrite=False))
