#!/usr/bin/env python

"""
:Mod: pasta_db

:Synopsis:
    Connection factory for the PASTA resource registry PostgreSQL database.

:Author:
    servilla

:Created:
    3/19/26
"""

import urllib.parse

import daiquiri
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from pinchme.config import Config

logger = daiquiri.getLogger(__name__)

_engine: Engine | None = None


def get_engine(db_url: str | None = None) -> Engine:
    """Return a cached SQLAlchemy Engine for the PASTA resource registry.

    Args:
        db_url: Optional database URL. If provided, creates an engine for that
            URL directly (useful for testing / DI). If None, builds the URL
            from Config values and caches the result.
    """
    global _engine

    if db_url is not None:
        return create_engine(db_url)

    if _engine is None:
        url = (
            Config.DB_DRIVER
            + "://"
            + Config.DB_USER
            + ":"
            + urllib.parse.quote_plus(Config.DB_PW)
            + "@"
            + Config.DB_HOST
            + "/"
            + Config.DB_DB
        )
        _engine = create_engine(url)
        logger.info("Created PASTA database engine")

    return _engine


def reset_engine() -> None:
    """Dispose of the cached engine. Intended for test teardown."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
