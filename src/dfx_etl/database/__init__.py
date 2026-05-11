"""
Routines for interacting with the database. The primary supported backend is PostgreSQL. If it is
not configured via a connection string in environment variables, an in-memory SQLite will be used.
"""

import logging
from functools import lru_cache
from pandas.io.sql import SQLTable
from sqlalchemy import Connection, Engine, create_engine, text, inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import make_url
from ..settings import SETTINGS
from .entities import *  # Required to register tables

__all__ = [
    "get_engine",
    "create_tables",
    "_drop_tables",
    "update_on_conflict",
    "ignore_on_conflict",
]


logger = logging.getLogger(__name__)

@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Get a database engine.

    If PostgresDsn is not configured, an in-memory SQLite database is used.

    Returns
    -------
    Engine
        SQLAlchemy database engine.
    """
    if SETTINGS.db_conn is None:
        logger.warning(
            "`DB_CONN` is not configured. Using an in-memory SQLite database."
        )
        engine = create_engine("sqlite:///:memory:")
    else:
        # 1. Parse the intended schema name from the connection string
        url = make_url(str(SETTINGS.db_conn))
        opts = url.query.get('options', '')
        intended_schema = None
        if 'search_path=' in opts:
            # Extracts 'dfx' from '-csearch_path=dfx'
            intended_schema = opts.split('search_path=')[1].split(',')[0].strip()
        engine = create_engine(str(SETTINGS.db_conn))

        if intended_schema:
            inspector = inspect(engine)
            existing_schemas = inspector.get_schema_names()

            if intended_schema not in existing_schemas:
                with engine.begin() as conn:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {intended_schema}"))
                    logger.info(f"Schema '{intended_schema}' created in {url.host}/{url.database}.")
            # 3. Patch the metadata so SQLAlchemy knows where to build tables
            Base.metadata.schema = intended_schema

    return engine


def create_tables(engine: Engine) -> list[str]:
    """
    Create tables in the database.

    This function also automatically populated the country table with M49 data.
    If the tables exist, they will not be recreated or altered.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy.Engine to use.

    Returns
    -------
    list[str]
        List of tables, excluding views, in the database.
    """
    logger.info("Creating tables in the database...")
    Base.metadata.create_all(engine)
    logger.info("Tables have been created.")
    return list(Base.metadata.tables.keys())


def _drop_tables(engine: Engine) -> None:
    """
    Drop tables in the database. Use with caution.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy.Engine to use.

    Returns
    -------
    None
        Nothing is returned.
    """
    logger.warning("Dropping tables in the database...")
    with engine.connect() as connection:
        connection.execute(text("DROP VIEW IF EXISTS observation;"))
    Base.metadata.drop_all(engine)
    logger.warning("Tables have been dropped.")


def update_on_conflict(
    table: SQLTable, conn: Connection, keys: list, data_iter: zip
) -> int:
    """
    Upsert logic for `method` argument in `pandas.to_sql` when there is a primary key conflict.

    The function is experimental and may not work as expected.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    result = conn.execute(upsert_statement)
    return result.rowcount


def ignore_on_conflict(
    table: SQLTable, conn: Connection, keys: list, data_iter: zip
) -> int:
    """
    Resolution logic for `method` argument in `pandas.to_sql` when there is a conflict on "name".

    The function is experimental and may not work as expected.

    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table).values(data).on_conflict_do_nothing(index_elements=["name"])
    )
    result = conn.execute(stmt)
    return result.rowcount
