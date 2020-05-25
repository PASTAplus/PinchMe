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
import logging
import os

import click
import daiquiri
from sqlalchemy.exc import IntegrityError

from pinchme.config import Config
from pinchme import package_pool
from pinchme import pasta_resource_registry
from pinchme import validation
from pinchme.lock import Lock
from pinchme.model.resource_db import ResourcePool


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
    "resource_type='dataPackage' AND package_id='<PID>'"
)

SQL_PACKAGES = (
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


help_limit = "Query limit to PASTA+ resource registry."
help_alg = (
    "Package pool selection algorithm: either 'random', 'id_ascending', "
    "'id_descending', 'create_ascending', or 'create_descending'."
)
help_package = (
                   "Add package (and resources) to validation pool. WARNING: "
                   "adding data packages out of sequence may alter how new "
                   "data packages are added to the package pool."
)
help_reset = "Reset validated flag on all packages and resources."


@click.command()
@click.option("-l", "--limit", default=100, help=help_limit)
@click.option("-a", "--algorithm", default="create_ascending", help=help_alg)
@click.option("-p", "--package", default=None, help=help_package)
@click.option("-r", "--reset", default=False, is_flag=True, help=help_reset)
def main(limit: int, algorithm: str, package: str, reset: bool):
    """
        PinchMe

        \b
        A PASTA+ data package integrity checker
    """
    lock = Lock(Config.LOCK_FILE)
    if lock.locked:
        logger.error("Lock file {} exists, exiting...".format(lock.lock_file))
        return 1
    else:
        lock.acquire()
        logger.info("Lock file {} acquired".format(lock.lock_file))

    rp = ResourcePool(Config.PINCHME_DB)

    if reset:
        rp.set_unvalidated_packages()
        rp.set_unvalidated_resources()
    else:
        # Update local resource pool with next set of package identifiers
        d = rp.get_last_package_create_date()
        if d is None:
            iso = Config.START_DATE.isoformat()
        else:
            iso = d.isoformat()

        # Update resources for all new package identiifers in the resource pool
        if package is not None:
            sql = SQL_PACKAGE.replace("<PID>", package)
        else:
            sql = SQL_PACKAGES.replace("<DATE>", iso).replace(
                "<LIMIT>", str(limit)
            )
        packages = pasta_resource_registry.query(sql)
        for package in packages:
            try:
                rp.insert_package(package[0], package[1])
            except IntegrityError as e:
                msg = f"Ignoring package '{package[0]}"
                logger.warning(msg)
            sql = SQL_RESOURCE.replace("<PID>", package[0])
            resources = pasta_resource_registry.query(sql)
            for resource in resources:
                try:
                    rp.insert_resource(
                        resource[0],
                        package[0],
                        resource[1],
                        resource[2],
                        resource[3],
                        resource[4],
                    )
                except IntegrityError as e:
                    msg = f"Ignoring resource '{resource[0]}'"
                    logger.warning(msg)

        packages = package_pool.get_unvalidated(rp, algorithm=algorithm)
        if packages is None:
            msg = "No new data packages detected"
            logger.warning(msg)
        for package in packages:
            resources = rp.get_package_resources(package.id)
            for resource in resources:
                if not resource.validated:
                    status = validation.valid_md5(resource)
                    date = datetime.now()
                    count = resource.checked_count + 1
                    rp.set_status_resource(resource.id, count, date, status)
                    rp.set_validated_resource(resource.id)
            rp.set_validated_package(package.id)

    lock.release()
    logger.info("Lock file {} released".format(lock.lock_file))

    return 0


if __name__ == "__main__":
    main()
