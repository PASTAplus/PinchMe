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
import os
from pathlib import Path
from datetime import datetime

import click
import daiquiri

from pinchme.config import Config
from pinchme import package_pool
from pinchme import validation
from pinchme.lock import Lock


cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/pinchme.log"
daiquiri.setup(
    level=Config.LOG_LEVEL, outputs=(daiquiri.output.File(Config.LOG_FILE), "stdout",)
)
logger = daiquiri.getLogger(__name__)


help_algorithm = (
    "Package pool selection algorithm: either 'create_ascending', 'create_descending', 'id_ascending', "
    " 'id_descending', or 'random' (default)."
)
help_bootstrap = "Create a new resource database and run validation against all packages."
help_delay = "Delay (seconds) between package integrity checks."
help_email = "Email on integrity error."
help_failed = "Rerun integrity checks against all failed resources, then exit."
help_limit = "Limit the number of new packages added to the validation pool."
help_identifier = "Add specified package to validation pool, then exit."
help_pool = "Add new packages to validation pool, then exit."
help_reset = "Reset validated flag on all packages/resources, then exit."
help_show = "Show all failed resources. Error codes: 0b0001=MD5 || 0b0010=SHA1 || 0b0100=SIZE || 0b1000=NOTFOUND"
help_validate = "Validate specified package(s), then exit."
help_verbose = "Print activity to stdout."

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("-a", "--algorithm", default="random", help=help_algorithm)
@click.option("-b", "--bootstrap", default=False, is_flag=True, help=help_bootstrap)
@click.option("-d", "--delay", default=0, help=help_delay)
@click.option("-e", "--email", default=False, is_flag=True, help=help_email)
@click.option("-f", "--failed", default=False, is_flag=True, help=help_failed)
@click.option("-i", "--identifier", default=None, help=help_identifier)
@click.option("-l", "--limit", default=0, help=help_limit)
@click.option("-p", "--pool", default=False, is_flag=True, help=help_pool)
@click.option("-r", "--reset", default=False, is_flag=True, help=help_reset)
@click.option("-s", "--show", default=False, is_flag=True, help=help_show)
@click.option("-v", "--validate", multiple=True, help=help_validate)
@click.option("-V", "--verbose", count=True, default=0, help=help_verbose)
def main(
        algorithm: str,
        bootstrap: bool,
        delay: int,
        email: bool,
        failed: bool,
        identifier: str,
        limit: int,
        pool: bool,
        reset: bool,
        show: bool,
        validate: tuple,
        verbose: int
):
    """
        PinchMe

        \b
        A PASTA data package and resource integrity checker. Running "pinchme" without any options will
        validate all new and existing data packages and resources, and then exit.
    """

    if reset:
        package_pool.reset_all()
        return 0

    if failed:
        validation.recheck_failed_resources(delay, email, verbose)
        return 0

    if show:
        resources = validation.show_failed_resources()
        if resources:
            print("pid, resource_id, checked_count, checked_last_date, checked_last_status")
        else:
            print("No failed resources")
        for resource in resources:
            msg = (
                f"{resource.pid}, {resource.id}, {resource.checked_count}, {resource.checked_last_date}, "
                f"0b{resource.checked_last_status:04b}"
            )
            print(msg)
        return 0

    if pool:
        package_pool.add_new_packages(identifier, limit, verbose)
        return 0

    if len(validate) > 0:
        for pid in validate:
            package = package_pool.get_package(pid)
            validation.integrity_check_packages(package, delay, email, verbose)
        return 0

    if bootstrap:
        # Validate all package resources
        lock = Lock(Config.LOCK_FILE)
        if lock.locked:
            logger.error(f"Lock file {lock.lock_file} exists, exiting...")
            return 1
        else:
            start = f"Bootstrap validation started: {datetime.now().isoformat()}"
            logger.warn(start)

            lock.acquire()
            logger.warn(f"Lock file {lock.lock_file} acquired")

            # Add all packages to the validation pool, validate, then exit
            Path(Config.PINCHME_DB).unlink(missing_ok=True)
            package_pool.add_new_packages(identifier, limit, verbose)
            packages = package_pool.get_packages(algorithm="create_ascending")
            validation.integrity_check_packages(packages, delay, email, verbose)

            lock.release()
            logger.warn(f"Lock file {lock.lock_file} released")

            end = f"Bootstrap validation ended: {datetime.now().isoformat()}"
            logger.warn(end)

            return 0

    # Validate all new and existing package resources
    lock = Lock(Config.LOCK_FILE)
    if lock.locked:
        logger.error(f"Lock file {lock.lock_file} exists, exiting...")
        return 1
    else:
        start = f"Bootstrap validation started: {datetime.now().isoformat()}"
        logger.warn(start)

        lock.acquire()
        logger.info(f"Lock file {lock.lock_file} acquired")

        # Add new packages to the validation pool and validate first
        package_pool.add_new_packages(identifier, limit, verbose)
        packages = package_pool.get_unvalidated(algorithm="create_ascending")
        validation.integrity_check_packages(packages, delay, email, verbose)

        # Validate all packages and their resources
        packages = package_pool.get_packages(algorithm)
        validation.integrity_check_packages(packages, delay, email, verbose)

        lock.release()
        logger.info(f"Lock file {lock.lock_file} released")

        end = f"Bootstrap validation ended: {datetime.now().isoformat()}"
        logger.warn(end)

    return 0


if __name__ == "__main__":
    main()
