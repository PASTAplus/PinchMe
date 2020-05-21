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
import daiquiri
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound

from pinchme.config import Config


logger = daiquiri.getLogger(__name__)


def query(sql: str) -> list:
    rs = list()
    db = Config.DB_DRIVER + '://' + Config.DB_USER + ':' + Config.DB_PW + '@'\
         + Config.DB_HOST + '/' + Config.DB_DB
    connection = create_engine(db)
    try:
        rs = connection.execute(sql).fetchall()
    except NoResultFound as e:
        logger.warning(e)
    except Exception as e:
        logger.error(e)
    return rs
