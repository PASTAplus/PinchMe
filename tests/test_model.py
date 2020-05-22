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
RESOURCE_TYPE = "data"
RESOURCE_NAME = "482fef41e108b34ad816e96423711470"
RESOURCE_MD5 = "9cbb385955353ef614b8f300602c4b8c"
RESOURCE_SHA1 = "f494400be17e301e9388bd542cfb9b2c91caaef3"

TEST_PACKAGE_DATA = [
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

TEST_RESOURCE_DATA = [
    (
        "https://pasta.lternet.edu/package/metadata/eml/edi/1/1",
        "edi.1.1",
        "metadata",
        None,
        "412e30ed380fafdf8654ae7495d1f3f6",
        "e698c128e4fdbc18767e467026349afbb30423db",
    ),
    (
        "https://pasta.lternet.edu/package/report/eml/edi/1/1",
        "edi.1.1",
        "report",
        None,
        "711efe73e09a02832d693c23abefb94f",
        "5c4e60e12d814148b7647f348c362ab4b4678245",
    ),
    (
        "https://pasta.lternet.edu/package/data/eml/edi/1/1"
        "/482fef41e108b34ad816e96423711470",
        "edi.1.1",
        "data",
        "482fef41e108b34ad816e96423711470",
        "9cbb385955353ef614b8f300602c4b8c",
        "f494400be17e301e9388bd542cfb9b2c91caaef3",
    ),
    (
        "https://pasta.lternet.edu/package/data/eml/edi/1/1"
        "/cba4645e845957d015008e7bccf4f902",
        "edi.1.1",
        "data",
        "cba4645e845957d015008e7bccf4f902",
        "85a4466056f5fce9255ca28fe6d1827a",
        "c1d6e1099634dd36f4e40b7da2558b12dc45b149",
    ),
]


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
    rp.insert_resource(
        RESOURCE_ID,
        PACKAGE_ID,
        RESOURCE_TYPE,
        RESOURCE_NAME,
        RESOURCE_MD5,
        RESOURCE_SHA1,
    )
    r = rp.get_resource(RESOURCE_ID)
    assert isinstance(r, Resources)
    assert r.id == RESOURCE_ID
    assert r.pid == PACKAGE_ID
    assert r.type == RESOURCE_TYPE
    assert r.entity_id == RESOURCE_NAME
    assert r.md5 == RESOURCE_MD5
    assert r.sha1 == RESOURCE_SHA1


def test_get_last_package_create_date(rp, clean_up):
    for package in TEST_PACKAGE_DATA:
        rp.insert_package(package[0], package[1])
    d = rp.get_last_package_create_date()
    assert d is not None
    assert isinstance(d, datetime)
    assert d == datetime(2017, 3, 10, 14, 13, 1, 911000)


def test_get_last_package_create_date_from_empty_db(rp, clean_up):
    d = rp.get_last_package_create_date()
    assert d is None


def test_set_dirty_package(rp, clean_up):
    rp.insert_package(PACKAGE_ID, pendulum.now())
    p = rp.get_package(PACKAGE_ID)
    assert not p.dirty
    rp.set_dirty_package(PACKAGE_ID)
    p = rp.get_package(PACKAGE_ID)
    assert p.dirty


def test_get_clean_packages(rp, clean_up):
    pre = len(TEST_PACKAGE_DATA)
    for package in TEST_PACKAGE_DATA:
        rp.insert_package(package[0], package[1])
    rp.set_dirty_package(PACKAGE_ID)
    p = rp.get_clean_packages()
    post = len(p)
    assert pre == post + 1


def test_set_dirty_resource(rp, clean_up):
    rp.insert_resource(
        RESOURCE_ID,
        PACKAGE_ID,
        RESOURCE_TYPE,
        RESOURCE_NAME,
        RESOURCE_MD5,
        RESOURCE_SHA1,
    )
    r = rp.get_resource(RESOURCE_ID)
    assert not r.dirty
    rp.set_dirty_resource(RESOURCE_ID)
    r = rp.get_resource(RESOURCE_ID)
    assert r.dirty


def test_get_clean_resources(rp, clean_up):
    pre = len(TEST_RESOURCE_DATA)
    for resource in TEST_RESOURCE_DATA:
        rp.insert_resource(
            resource[0],
            resource[1],
            resource[2],
            resource[3],
            resource[4],
            resource[5],
        )
    rp.set_dirty_resource(RESOURCE_ID)
    r = rp.get_clean_resources()
    post = len(r)
    assert pre == post + 1


def test_clean_all_resources(rp, clean_up):
    for resource in TEST_RESOURCE_DATA:
        rp.insert_resource(
            resource[0],
            resource[1],
            resource[2],
            resource[3],
            resource[4],
            resource[5],
        )
        rp.set_dirty_resource(resource[0])
    for resource in TEST_RESOURCE_DATA:
        r = rp.get_resource(resource[0])
        assert r.dirty
    rp.set_clean_resources()
    for resource in TEST_RESOURCE_DATA:
        r = rp.get_resource(resource[0])
        assert not r.dirty


def test_get_package_resources(rp, clean_up):
    for resource in TEST_RESOURCE_DATA:
        rp.insert_resource(
            resource[0],
            resource[1],
            resource[2],
            resource[3],
            resource[4],
            resource[5],
        )
    resources = rp.get_package_resources(PACKAGE_ID)
    for resource in resources:
        assert PACKAGE_ID in resource.pid


def test_set_status_resource(rp, clean_up):
    rp.insert_resource(
        RESOURCE_ID,
        PACKAGE_ID,
        RESOURCE_TYPE,
        RESOURCE_NAME,
        RESOURCE_MD5,
        RESOURCE_SHA1,
    )
    r = rp.get_resource(RESOURCE_ID)
    status = True
    date = datetime.now()
    count = r.checked_count + 1
    rp.set_status_resource(r.id, count, date, status)

    r = rp.get_resource(RESOURCE_ID)
    assert r.checked_last_status
    assert r.checked_last_date == date
    assert r.checked_count == count
