#!/usr/bin/env python

"""
:Mod: test_validation

:Synopsis:

:Author:
    servilla

:Created:
    5/23/20
"""

from pathlib import Path

from pinchme import validation

PACKAGE_ID = "edi.1.1"

RESOURCE_ID = "https://pasta.lternet.edu/package/data/eml/edi/1/1/482fef41e108b34ad816e96423711470"
RESOURCE_TYPE = "data"
RESOURCE_NAME = "482fef41e108b34ad816e96423711470"
RESOURCE_MD5 = "9cbb385955353ef614b8f300602c4b8c"
RESOURCE_SHA1 = "f494400be17e301e9388bd542cfb9b2c91caaef3"
RESOURCE_SIZE = 1516906
RESOURCE_LOCATION = str(
    Path.cwd().resolve() / "data/edi.1.1/482fef41e108b34ad816e96423711470"
)


def test_valid_md5(rp, clean_up):
    rp.insert_resource(
        RESOURCE_ID,
        PACKAGE_ID,
        RESOURCE_TYPE,
        RESOURCE_NAME,
        RESOURCE_MD5,
        RESOURCE_SHA1,
        RESOURCE_SIZE,
        RESOURCE_LOCATION,
    )
    r = rp.get_resource(RESOURCE_ID)
    assert validation.valid_md5(r)
