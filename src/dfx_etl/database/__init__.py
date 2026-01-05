"""
Routines for interacting with the database. The primary supported backend is PostgreSQL. If it is
not configured via a connection string in environment variables, an in-memory SQLite will be used.
"""

import logging

from sqlalchemy import Engine, create_engine, text

from ..settings import SETTINGS
from .entities import *  # Required to register tables

__all__ = ["get_engine", "create_tables", "_drop_tables"]


logger = logging.getLogger(__name__)


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
        engine = create_engine(str(SETTINGS.db_conn))
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
