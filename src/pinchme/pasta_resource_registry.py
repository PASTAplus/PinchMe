#!/usr/bin/env python

"""
:Mod: pasta_resource_registry

:Synopsis:
    Query execution and SQL constants for the PASTA resource registry.

:Author:
    servilla

:Created:
    5/12/20
"""

import time

import daiquiri
from sqlalchemy import text
from sqlalchemy.engine import Engine, Row
from sqlalchemy.exc import OperationalError

logger = daiquiri.getLogger(__name__)


# ---------------------------------------------------------------------------
# PASTA resource registry SQL constants
# ---------------------------------------------------------------------------

SQL_PACKAGE = (
    "SELECT datapackagemanager.resource_registry.package_id, "
    "datapackagemanager.resource_registry.date_created "
    "FROM datapackagemanager.resource_registry WHERE "
    "resource_type='dataPackage' AND package_id='<PID>'"
)

SQL_PACKAGES = (
    "SELECT datapackagemanager.resource_registry.package_id, "
    "datapackagemanager.resource_registry.date_created "
    "FROM datapackagemanager.resource_registry WHERE "
    "resource_type='dataPackage' AND date_created > '<DATE>' "
    "ORDER BY date_created ASC LIMIT <LIMIT>"
)

SQL_PACKAGES_NO_LIMIT = (
    "SELECT datapackagemanager.resource_registry.package_id, "
    "datapackagemanager.resource_registry.date_created "
    "FROM datapackagemanager.resource_registry WHERE "
    "resource_type='dataPackage' AND date_created > '<DATE>' "
    "ORDER BY date_created ASC"
)

SQL_RESOURCE = (
    "SELECT datapackagemanager.resource_registry.resource_id, "
    "datapackagemanager.resource_registry.resource_type, "
    "datapackagemanager.resource_registry.entity_id, "
    "datapackagemanager.resource_registry.md5_checksum, "
    "datapackagemanager.resource_registry.sha1_checksum, "
    "datapackagemanager.resource_registry.resource_size, "
    "datapackagemanager.resource_registry.resource_location "
    "FROM datapackagemanager.resource_registry "
    "WHERE resource_type<>'dataPackage' AND package_id='<PID>'"
)


def query(engine: Engine, sql: str, retries: int = 3, delay: int = 5) -> list[Row]:
    """Execute a raw SQL query against the PASTA resource registry.

    Args:
        engine: A SQLAlchemy Engine (obtain via ``pasta_db.get_engine()``).
        sql: The SQL string to execute.
        retries: Number of connection attempts before raising.
        delay: Seconds to wait between retries.
    """
    attempt = 0
    while attempt < retries:
        try:
            with engine.connect() as connection:
                rs = connection.execute(text(sql)).fetchall()
            return rs
        except OperationalError as e:
            msg = f"Connection attempt {attempt + 1} failed: {e}"
            logger.warning(msg)
            attempt += 1
            if attempt < retries:
                logger.warning(f"Retrying in {delay} seconds")
                time.sleep(delay)
    raise OperationalError(f"Failed to connect to database after {retries} attempts")
