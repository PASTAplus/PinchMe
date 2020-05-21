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
import pathlib

import pendulum
import pytest

from pinchme.config import Config
from pinchme.model.resource_db import ResourcePool


ID = "edi.1.1"


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
    rp.insert_package(ID, pendulum.now())
    p = rp.get_package(ID)
    assert p.id == ID
