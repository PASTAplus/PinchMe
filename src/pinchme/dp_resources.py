#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: dp_resources

:Synopsis:

:Author:
    servilla

:Created:
    5/12/20
"""
import daiquiri
from sqlalchemy import create_engine

from pinchme.config import Config


logger = daiquiri.getLogger(__name__)


def get_packages(sql: str):
    db = Config.DB_DRIVER + '://' + Config.DB_USER + ':' + Config.DB_PW + '@'\
         + Config.DB_HOST + '/' + Config.DB_DB
    connection = create_engine(db)
    try:
        rs = connection.execute(sql).fetchall()
        return rs
    except Exception as e:
        logger.error(e)
