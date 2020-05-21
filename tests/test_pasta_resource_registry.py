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

from pinchme import pasta_resource_registry


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

    packages = pasta_resource_registry.query(sql)
    assert len(packages) == 10
    for package in packages:
        assert package in test_data


def test_resource_query():
    test_data = [
        (
            "https://pasta.lternet.edu/package/metadata/eml/edi/1/1",
            "metadata",
            None,
            "412e30ed380fafdf8654ae7495d1f3f6",
            "e698c128e4fdbc18767e467026349afbb30423db",
        ),
        (
            "https://pasta.lternet.edu/package/report/eml/edi/1/1",
            "report",
            None,
            "711efe73e09a02832d693c23abefb94f",
            "5c4e60e12d814148b7647f348c362ab4b4678245",
        ),
        (
            "https://pasta.lternet.edu/package/data/eml/edi/1/1/482fef41e108b34ad816e96423711470",
            "data",
            "482fef41e108b34ad816e96423711470",
            "9cbb385955353ef614b8f300602c4b8c",
            "f494400be17e301e9388bd542cfb9b2c91caaef3",
        ),
        (
            "https://pasta.lternet.edu/package/data/eml/edi/1/1/cba4645e845957d015008e7bccf4f902",
            "data",
            "cba4645e845957d015008e7bccf4f902",
            "85a4466056f5fce9255ca28fe6d1827a",
            "c1d6e1099634dd36f4e40b7da2558b12dc45b149",
        ),
    ]

    sql = (
        "SELECT datapackagemanager.resource_registry.resource_id, "
        "datapackagemanager.resource_registry.resource_type, "
        "datapackagemanager.resource_registry.entity_id, "
        "datapackagemanager.resource_registry.md5_checksum, "
        "datapackagemanager.resource_registry.sha1_checksum "
        "FROM datapackagemanager.resource_registry "
        "WHERE resource_type<>'dataPackage' AND package_id='edi.1.1'"
    )

    resources = pasta_resource_registry.query(sql)
    assert len(resources) == 4
    for resource in resources:
        assert resource in test_data
