#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: validation

:Synopsis:

:Author:
    servilla

:Created:
    5/23/20
"""
from datetime import datetime
import hashlib
from pathlib import Path

import daiquiri
from sqlalchemy.orm.query import Query

from pinchme.config import Config
from pinchme.model.resource_db import Resources, ResourcePool


logger = daiquiri.getLogger(__name__)


def integrity_check_packages(packages: Query):
    rp = ResourcePool(Config.PINCHME_DB)
    if packages is None:
        msg = "No new data packages detected"
        logger.warning(msg)
    for package in packages:
        resources = rp.get_package_resources(package.id)
        for resource in resources:
            if not resource.validated:
                status = valid_md5(resource)
                date = datetime.now()
                count = resource.checked_count + 1
                rp.set_status_resource(resource.id, count, date, status)
                rp.set_validated_resource(resource.id)
        rp.set_validated_package(package.id)


def recheck_failed_resources():
    rp = ResourcePool(Config.PINCHME_DB)
    resources = rp.get_failed_resources()
    for resource in resources:
        status = valid_md5(resource)
        date = datetime.now()
        count = resource.checked_count + 1
        rp.set_status_resource(resource.id, count, date, status)
        rp.set_validated_resource(resource.id)


def show_failed_resources() -> Query:
    rp = ResourcePool(Config.PINCHME_DB)
    return rp.get_failed_resources()


def valid_md5(resource: Resources) -> bool:
    status = False
    path_head = f"{Config.DATA_STORE}/{resource.pid}"
    if resource.type == "metadata":
        resource_path = f"{path_head}/Level-1-EML.xml"
    elif resource.type == "report":
        resource_path = f"{path_head}/quality_report.xml"
    else:  # resource.typ == "data"
        resource_path = f"{path_head}/{resource.entity_id}"

    if Path(resource_path).exists():
        msg = f"Validating checksum for resource '{resource_path}'"
        logger.info(msg)
        with open(resource_path, "rb") as f:
            md5_hash = hashlib.md5()
            while chunk := f.read(8192):
                md5_hash.update(chunk)
            md5 = md5_hash.hexdigest()
        if md5 == resource.md5:
            status = True
            msg = f"Resource '{resource_path}' is valid"
            logger.info(msg)
        else:
            msg = (
                f"Resource '{resource_path}' is not valid; "
                f"expected '{resource.md5}', but got '{md5}'"
            )
            logger.error(msg)
    else:
        msg = f"Resource '{resource_path}' not found"
        logger.error(msg)
    return status


