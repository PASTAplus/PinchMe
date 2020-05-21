#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: test_pasta_resource_registry

:Synopsis:

:Author:
    servilla

:Created:
    5/20/20
"""
import datetime

import pytest

from pinchme import dp_resources


def test_package_query():
    test_data = [
        ("edi.1.1", datetime.datetime(2016, 12, 1, 12, 55, 8, 778000)),
        ("edi.2.1", datetime.datetime(2016, 12, 1, 13, 15, 38, 166000)),
        ("edi.3.1", datetime.datetime(2016, 12, 21, 15, 53, 42, 211000)),
        ("edi.4.1", datetime.datetime(2017, 1, 2, 10, 29, 4, 33000)),
        ("edi.7.1", datetime.datetime(2017, 1, 7, 10, 39, 36, 319000)),
        ("edi.5.1", datetime.datetime(2017, 1, 7, 11, 20, 53, 526000)),
        ("edi.6.1", datetime.datetime(2017, 1, 7, 12, 12, 1, 678000)),
        ("edi.4.2", datetime.datetime(2017, 2, 13, 12, 56, 59, 337000)),
        ("edi.8.1", datetime.datetime(2017, 3, 5, 17, 43, 4, 215000)),
        ("edi.8.2", datetime.datetime(2017, 3, 10, 14, 13, 1, 911000)),
    ]

    sql = (
        "SELECT datapackagemanager.resource_registry.package_id, "
        "datapackagemanager.resource_registry.date_created "
        "FROM datapackagemanager.resource_registry WHERE "
        "resource_type='dataPackage' AND scope='edi' "
        "ORDER BY date_created ASC LIMIT 10"
    )

    packages = dp_resources.get_packages(sql)
    assert len(packages) == 10
    for package in packages:
        assert package in test_data
