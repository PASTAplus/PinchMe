#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: test_model

:Synopsis:

:Author:
    servilla

:Created:
    5/20/20
"""
from datetime import datetime
import pathlib

import pendulum
import pytest

from pinchme.config import Config
from pinchme.model.resource_db import ResourcePool, Packages, Resources


PACKAGE_ID = "edi.1.1"
RESOURCE_ID = "https://pasta.lternet.edu/package/data/eml/edi/1/1/482fef41e108b34ad816e96423711470"
RESOURCE_MD5 = "9cbb385955353ef614b8f300602c4b8c"
RESOURCE_SHA1 = "f494400be17e301e9388bd542cfb9b2c91caaef3"


@pytest.fixture()
def rp():
    resource_pool = ResourcePool(Config.PINCHME_TEST_DB)
    return resource_pool


@pytest.fixture()
def clean_up():
    yield
    pathlib.Path(Config.PINCHME_TEST_DB).unlink(missing_ok=True)


def test_pasta_db_connection():
    pass


def test_resource_pool_create(rp, clean_up):
    assert pathlib.Path(Config.PINCHME_TEST_DB).exists()


def test_insert_get_package(rp, clean_up):
    rp.insert_package(PACKAGE_ID, pendulum.now())
    p = rp.get_package(PACKAGE_ID)
    assert isinstance(p, Packages)
    assert p.id == PACKAGE_ID


def test_insert_get_resource(rp, clean_up):
    rp.insert_resource(RESOURCE_ID, RESOURCE_MD5, RESOURCE_SHA1)
    r = rp.get_resource(RESOURCE_ID)
    assert isinstance(r, Resources)
    assert r.id == RESOURCE_ID
    assert r.md5 == RESOURCE_MD5
    assert r.sha1 == RESOURCE_SHA1


def test_get_last_package_create_date(rp, clean_up):
    test_data = [
        ("edi.1.1", datetime(2016, 12, 1, 12, 55, 8, 778000)),
        ("edi.2.1", datetime(2016, 12, 1, 13, 15, 38, 166000)),
        ("edi.3.1", datetime(2016, 12, 21, 15, 53, 42, 211000)),
        ("edi.4.1", datetime(2017, 1, 2, 10, 29, 4, 33000)),
        ("edi.7.1", datetime(2017, 1, 7, 10, 39, 36, 319000)),
        ("edi.5.1", datetime(2017, 1, 7, 11, 20, 53, 526000)),
        ("edi.6.1", datetime(2017, 1, 7, 12, 12, 1, 678000)),
        ("edi.4.2", datetime(2017, 2, 13, 12, 56, 59, 337000)),
        ("edi.8.1", datetime(2017, 3, 5, 17, 43, 4, 215000)),
        ("edi.8.2", datetime(2017, 3, 10, 14, 13, 1, 911000)),
    ]

    for package in test_data:
        rp.insert_package(package[0], package[1])

    d = rp.get_last_package_create_date()
    assert d is not None
    assert isinstance(d, datetime)
    assert d == datetime(2017, 3, 10, 14, 13, 1, 911000)

