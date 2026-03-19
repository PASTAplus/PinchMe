#!/usr/bin/env python

"""
:Mod: package_pool

:Synopsis:

:Author:
    servilla

:Created:
    5/23/20
"""

import daiquiri
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.query import Query

from pinchme.config import Config
from pinchme.model.resource_db import ResourcePool
from pinchme.pasta_db import get_engine
from pinchme.pasta_resource_registry import (
    SQL_PACKAGE,
    SQL_PACKAGES,
    SQL_PACKAGES_NO_LIMIT,
    SQL_RESOURCE,
)
from pinchme.pasta_resource_registry import (
    query as registry_query,
)

logger = daiquiri.getLogger(__name__)


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
        sql = SQL_PACKAGE
        params = {"pid": identifier}
    else:
        if limit > 0:
            sql = SQL_PACKAGES
            params = {"date": iso, "limit": limit}
        else:
            sql = SQL_PACKAGES_NO_LIMIT
            params = {"date": iso}
    engine = get_engine()
    packages = registry_query(engine, sql, params=params)
    if len(packages) == 0 and identifier is None:
        msg = "No new packages to add to the pool"
        logger.warning(msg)
    elif len(packages) == 0 and identifier is not None:
        msg = f"Package '{identifier}' not found in resource registry"
        logger.warning(msg)
    else:
        for package in packages:
            if verbose >= 1:
                print(f"Adding package '{package[0]}'")
            try:
                rp.insert_package(package[0], package[1])
            except IntegrityError:
                msg = f"Package found in package pool - skipping package '{package[0]}'"
                logger.warning(msg)
                break
            resources = registry_query(engine, SQL_RESOURCE, params={"pid": package[0]})
            for resource in resources:
                if verbose >= 3:
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
                        resource[6],
                    )
                except IntegrityError:
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
    if package is not None:
        rp = ResourcePool(Config.PINCHME_DB)
        engine = get_engine()
        resources = registry_query(engine, SQL_RESOURCE, params={"pid": pid})
        for resource in resources:
            rp.update_resource(resource[0], resource[3], resource[4], resource[5])
            if verbose >= 1:
                print(
                    f"Updating resource {resource[0]}: {resource[3]}, {resource[4]}, {resource[5]}"
                )
    else:
        msg = f"Package '{pid}' not in package pool"
        logger.warning(msg)
