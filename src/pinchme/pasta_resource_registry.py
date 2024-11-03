#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: pasta_resource_registry

:Synopsis:

:Author:
    servilla

:Created:
    5/12/20
"""
import time
import urllib.parse

import daiquiri
from sqlalchemy import create_engine
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.exc import NoResultFound

from pinchme.config import Config


logger = daiquiri.getLogger(__name__)


def query(sql: str, retries: int=3, delay: int=5) -> ResultProxy:
    rs = None
    db = (
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
    connection = create_engine(db)
    attempt = 0
    while attempt < retries:
        try:
            rs = connection.execute(sql).fetchall()
            return rs
        except OperationalError as e:
            msg = f"Connection attempt {attempt + 1} failed: {e}"
            logger.warning(msg)
            attempt += 1
            if attempt < retries:
                logger.warning(f"Retrying in {delay} seconds")
                time.sleep(delay)
    raise OperationalError(f"Failed to connect to database after {retries} attempts")

