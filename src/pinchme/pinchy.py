#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: pinchy

:Synopsis:

:Author:
    servilla

:Created:
    5/12/20
"""
from datetime import datetime
import hashlib
import logging
import os
from pathlib import Path

import click
import daiquiri

from pinchme.config import Config
from pinchme import pasta_resource_registry
from pinchme.lock import Lock
from pinchme.model.resource_db import ResourcePool
from pinchme.model.resource_db import Resources

cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/pinchme.log"
daiquiri.setup(
    level=logging.INFO, outputs=(daiquiri.output.File(logfile), "stdout",)
)
logger = daiquiri.getLogger(__name__)

SQL_PACKAGE = (
    "SELECT datapackagemanager.resource_registry.package_id, "
    "datapackagemanager.resource_registry.date_created "
    "FROM datapackagemanager.resource_registry WHERE "
    "resource_type='dataPackage' AND date_created > '<DATE>' "
    "ORDER BY date_created ASC LIMIT <LIMIT>"
)

SQL_RESOURCE = (
    "SELECT datapackagemanager.resource_registry.resource_id, "
    "datapackagemanager.resource_registry.resource_type, "
    "datapackagemanager.resource_registry.entity_id, "
    "datapackagemanager.resource_registry.md5_checksum, "
    "datapackagemanager.resource_registry.sha1_checksum "
    "FROM datapackagemanager.resource_registry "
    "WHERE resource_type<>'dataPackage' AND package_id='<PID>'"
)


def valid_md5(resource: Resources) -> bool:
    status = False
    path_head = f"{Config.DATA_STORE}/{resource.pid}"
    if resource.type == "metadata":
        resource_path = f"{path_head}/Level-1-EML.xml"
    elif resource.type == "report":
        resource_path = f"{path_head}/quality_report.xml"
    else: # resource.typ == "data"
        resource_path = f"{path_head}/{resource.entity_id}"

    if Path(resource_path).exists():
        with open(resource_path, "rb") as f:
            data = f.read()
        md5 = hashlib.md5(data).hexdigest()
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


help_limit = "Query limit to PASTA+ resource registry"


@click.command()
@click.option("-l", "--limit", default=100, help=help_limit)
def main(limit: int):
    lock = Lock(Config.LOCK_FILE)
    if lock.locked:
        logger.error("Lock file {} exists, exiting...".format(lock.lock_file))
        return 1
    else:
        lock.acquire()
        logger.warning("Lock file {} acquired".format(lock.lock_file))

    # Update local resource pool with next set of package identifiers
    rp = ResourcePool(Config.PINCHME_DB)
    d = rp.get_last_package_create_date()
    if d is None:
        iso = Config.START_DATE.isoformat()
    else:
        iso = d.isoformat()

    # Update resources for all new package identiifers in the resource pool
    sql = SQL_PACKAGE.replace("<DATE>", iso).replace("<LIMIT>", str(limit))
    packages = pasta_resource_registry.query(sql)
    for package in packages:
        rp.insert_package(package[0], package[1])
        sql = SQL_RESOURCE.replace("<PID>", package[0])
        resources = pasta_resource_registry.query(sql)
        for resource in resources:
            rp.insert_resource(
                resource[0],
                package[0],
                resource[1],
                resource[2],
                resource[3],
                resource[4],
            )

    packages = rp.get_clean_packages()
    if packages is None:
        rp.set_clean_packages()
        rp.set_clean_resources()
    else:
        for package in packages:
            resources = rp.get_package_resources(package.id)
            for resource in resources:
                if not resource.dirty:
                    status = valid_md5(resource)
                    date = datetime.now()
                    count = resource.checked_count + 1
                    rp.set_status_resource(resource.id, count, date, status)
                    rp.set_dirty_resource(resource.id)
            rp.set_dirty_package(package.id)

    lock.release()
    logger.warning("Lock file {} released".format(lock.lock_file))

    return 0


if __name__ == "__main__":
    main()
