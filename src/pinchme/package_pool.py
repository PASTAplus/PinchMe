#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: package_pool

:Synopsis:

:Author:
    servilla

:Created:
    5/23/20
"""
import daiquiri

from pinchme.model.resource_db import ResourcePool

logger = daiquiri.getLogger(__name__)


def get_unvalidated(
    rp: ResourcePool, algorithm: str = "create_ascending"
) -> list:
    packages = list()
    if algorithm == "random":
        pass
    elif algorithm == "id_ascending":
        packages = rp.get_unvalidated_packages(col="id", order="asc")
    elif algorithm == "id_descending":
        packages = rp.get_unvalidated_packages(col="id", order="desc")
    elif algorithm == "create_ascending":
        packages = rp.get_unvalidated_packages(col="date_created", order="asc")
    elif algorithm == "create_descending":
        packages = rp.get_unvalidated_packages(col="date_created", order="desc")
    return packages
