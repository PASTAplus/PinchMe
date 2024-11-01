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
import logging
import os

import click
import daiquiri

from pinchme.config import Config
from pinchme import package_pool
from pinchme import validation
from pinchme.lock import Lock
from pinchme.model.resource_db import ResourcePool


cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/pinchme.log"
daiquiri.setup(
    level=logging.INFO, outputs=(daiquiri.output.File(Config.LOG_FILE), "stdout",)
)
logger = daiquiri.getLogger(__name__)


help_algorithm = (
    "Package pool selection algorithm: either 'create_ascending', 'create_descending', 'id_ascending', "
    " 'id_descending', or 'random' (default)."
)
help_delay = "Delay (seconds) between package integrity checks."
help_email = "Email on integrity error."
help_failed = "Rerun integrity checks against all failed resources, then exit."
help_limit = "Limit the number of new packages added to the validation pool."
help_identifier = "Add specified package to validation pool, then exit."
help_pool = "Add new packages to validation pool, then exit."
help_reset = "Reset validated flag on all packages/resources, then exit."
help_show = "Show all failed resources."
help_validate = "Validate specified package(s), then exit."

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("-a", "--algorithm", default="create_ascending", help=help_algorithm)
@click.option("-d", "--delay", default=0, help=help_delay)
@click.option("-e", "--email", default=False, is_flag=True, help=help_email)
@click.option("-f", "--failed", default=False, is_flag=True, help=help_failed)
@click.option("-i", "--identifier", default=None, help=help_identifier)
@click.option("-l", "--limit", default=0, help=help_limit)
@click.option("-p", "--pool", default=False, is_flag=True, help=help_pool)
@click.option("-r", "--reset", default=False, is_flag=True, help=help_reset)
@click.option("-s", "--show", default=False, is_flag=True, help=help_show)
@click.option("-v", "--validate", multiple=True, help=help_validate)
def main(
        algorithm: str,
        delay: int,
        email: bool,
        failed: bool,
        identifier: str,
        limit: int,
        pool: bool,
        reset: bool,
        show: bool,
        validate: tuple
):
    """
        PinchMe

        \b
        A PASTA+ data package integrity checker
    """

    if reset:
        package_pool.reset_all()
        return 0

    if failed:
        validation.recheck_failed_resources(delay)
        return 0

    if show:
        resources = validation.show_failed_resources()
        if resources:
            print("pid, resource_id, checked_count, checked_last_date")
        else:
            print("No failed resources")
        for resource in resources:
            msg = f"{resource.pid}, {resource.id}, {resource.checked_count}, {resource.checked_last_date}"
            print(msg)
        return 0

    if pool:
        package_pool.add_new_packages(identifier, limit)
        return 0

    if validate:
        for pid in validate:
            package = package_pool.get_package(pid)
            validation.integrity_check_packages(package, delay)
        return 0

    # Validate all package resources
    lock = Lock(Config.LOCK_FILE)
    if lock.locked:
        logger.error(f"Lock file {lock.lock_file} exists, exiting...")
        return 1
    else:
        lock.acquire()
        logger.info(f"Lock file {lock.lock_file} acquired")

        package_pool.add_new_packages(identifier, limit)
        packages = package_pool.get_unvalidated(algorithm)
        validation.integrity_check_packages(packages, delay)
        packages = package_pool.get_packages(algorithm)
        validation.integrity_check_packages(packages, delay)

        lock.release()
        logger.info(f"Lock file {lock.lock_file} released")

        return 0


if __name__ == "__main__":
    main()
