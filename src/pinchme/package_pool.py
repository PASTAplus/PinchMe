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

from sqlalchemy.orm.query import Query
from sqlalchemy.exc import IntegrityError

from pinchme import pasta_resource_registry
from pinchme.config import Config
from pinchme.model.resource_db import ResourcePool


logger = daiquiri.getLogger(__name__)

SQL_PACKAGE = ("SELECT datapackagemanager.resource_registry.package_id, "
               "datapackagemanager.resource_registry.date_created "
               "FROM datapackagemanager.resource_registry WHERE "
               "resource_type='dataPackage' AND package_id='<PID>'")

SQL_PACKAGES = ("SELECT datapackagemanager.resource_registry.package_id, "
                "datapackagemanager.resource_registry.date_created "
                "FROM datapackagemanager.resource_registry WHERE "
                "resource_type='dataPackage' AND date_created > '<DATE>' "
                "ORDER BY date_created ASC LIMIT <LIMIT>")

SQL_PACKAGES_NO_LIMIT = (
    "SELECT datapackagemanager.resource_registry.package_id, "
    "datapackagemanager.resource_registry.date_created "
    "FROM datapackagemanager.resource_registry WHERE "
    "resource_type='dataPackage' AND date_created > '<DATE>' "
    "ORDER BY date_created ASC")

SQL_RESOURCE = ("SELECT datapackagemanager.resource_registry.resource_id, "
                "datapackagemanager.resource_registry.resource_type, "
                "datapackagemanager.resource_registry.entity_id, "
                "datapackagemanager.resource_registry.md5_checksum, "
                "datapackagemanager.resource_registry.sha1_checksum, "
                "datapackagemanager.resource_registry.resource_size, "
                "datapackagemanager.resource_registry.resource_location "
                "FROM datapackagemanager.resource_registry "
                "WHERE resource_type<>'dataPackage' AND package_id='<PID>'")


def add_new_packages(identifier: str | None, limit: int, verbose: int):
    rp = ResourcePool(Config.PINCHME_DB)
    # Update local resource pool with next set of package identifiers
    d = rp.get_last_package_create_date()
    if d is None:
        iso = Config.START_DATE.isoformat()
    else:
        iso = d.isoformat()

    # Update resources for all new package identifiers in the resource pool
    if identifier is not None:
        sql = SQL_PACKAGE.replace("<PID>", identifier)
    else:
        if limit > 0:
            sql = SQL_PACKAGES.replace("<DATE>", iso).replace(
                "<LIMIT>", str(limit)
            )
        else:
            sql = SQL_PACKAGES_NO_LIMIT.replace("<DATE>", iso)
    packages = pasta_resource_registry.query(sql)
    for package in packages:
        if verbose >= 1:
            print(f"Adding package '{package[0]}'")
        try:
             rp.insert_package(package[0], package[1])
        except IntegrityError as e:
            msg = f"Ignoring package '{package[0]}"
            logger.warning(msg)
            break
        sql = SQL_RESOURCE.replace("<PID>", package[0])
        resources = pasta_resource_registry.query(sql)
        for resource in resources:
            if verbose >=3:
                print(f"Adding resource '{resource[0]}'")
            try:
                rp.insert_resource(
                    resource[0],
                    package[0],
                    resource[1],
                    resource[2],
                    resource[3],
                    resource[4],
                    resource[5],
                    resource[6]
                )
            except IntegrityError as e:
                msg = f"Ignoring resource '{resource[0]}'"
                logger.warning(msg)


def get_unvalidated(algorithm: str = "create_ascending") -> Query:
    rp = ResourcePool(Config.PINCHME_DB)
    if algorithm == "random":
        packages = rp.get_unvalidated_packages(col="id", order="asc")
    elif algorithm == "id_ascending":
        packages = rp.get_unvalidated_packages(col="id", order="asc")
    elif algorithm == "id_descending":
        packages = rp.get_unvalidated_packages(col="id", order="desc")
    elif algorithm == "create_ascending":
        packages = rp.get_unvalidated_packages(col="date_created", order="asc")
    else:  # algorithm == "create_descending":
        packages = rp.get_unvalidated_packages(col="date_created", order="desc")
    return packages


def get_packages(algorithm: str = "create_ascending") -> Query:
    rp = ResourcePool(Config.PINCHME_DB)
    if algorithm == "random":
        packages = rp.get_packages(col="id", order="random")
    elif algorithm == "id_ascending":
        packages = rp.get_packages(col="id", order="asc")
    elif algorithm == "id_descending":
        packages = rp.get_packages(col="id", order="desc")
    elif algorithm == "create_ascending":
        packages = rp.get_packages(col="date_created", order="asc")
    else:  # algorithm == "create_descending":
        packages = rp.get_packages(col="date_created", order="desc")
    return packages


def reset_all():
    rp = ResourcePool(Config.PINCHME_DB)
    rp.set_unvalidated_packages()
    logger.info("Reset all packages to unvalidated")
    rp.set_unvalidated_resources()
    logger.info("Reset all resources to unvalidated")


def get_package(pid: str) -> Query:
    rp = ResourcePool(Config.PINCHME_DB)
    return rp.get_package(pid)


def update_package(pid: str, verbose: int):
    package = get_package(pid)
    if package.count() == 1:
        rp = ResourcePool(Config.PINCHME_DB)
        sql = SQL_RESOURCE.replace("<PID>", pid)
        resources = pasta_resource_registry.query(sql)
        for resource in resources:
            rp.update_resource(
                resource[0],
                resource[3],
                resource[4],
                resource[5]
            )
            if verbose >= 1:
                print(f"Updating resource {resource[0]}: {resource[3]}, {resource[4]}, {resource[5]}")
    else:
        msg = f"Package '{pid}' not in package pool"
        logger.warning(msg)
