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
    level=logging.INFO, outputs=(daiquiri.output.File(logfile), "stdout",)
)
logger = daiquiri.getLogger(__name__)


help_limit = "Query limit to PASTA+ resource registry."
help_alg = (
    "Package pool selection algorithm: either 'random', 'id_ascending', "
    "'id_descending', 'create_ascending', or 'create_descending'."
)
help_pid = (
    "Add package (and resources) to validation pool. WARNING: "
    "adding data packages out of sequence may alter how new "
    "data packages are added to the package pool."
)
help_reset = "Reset validated flag on all packages/resources and exit."
help_failed = "Rerun integrity checks against all failed resources."
help_show = "Show all failed resources."


@click.command()
@click.option("-l", "--limit", default=None, help=help_limit)
@click.option("-a", "--algorithm", default="create_ascending", help=help_alg)
@click.option("-i", "--identifier", default=None, help=help_pid)
@click.option("-r", "--reset", default=False, is_flag=True, help=help_reset)
@click.option("-f", "--failed", default=False, is_flag=True, help=help_failed)
@click.option("-s", "--show", default=False, is_flag=True, help=help_show)
def main(
    limit: int,
    algorithm: str,
    identifier: str,
    reset: bool,
    failed: bool,
    show: bool,
):
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

    if reset:
        package_pool.reset_all()
    elif failed:
        validation.recheck_failed_resources()
    elif show:
        resources = validation.show_failed_resources()
        for resource in resources:
            msg = (
                f"{resource.pid}, {resource.id}, {resource.checked_count}, "
                f"{resource.checked_last_date}"
            )
            print(msg)
    else:
        package_pool.add_new_packages(identifier, limit)
        packages = package_pool.get_unvalidated(algorithm)
        validation.integrity_check_packages(packages)

    lock.release()
    logger.info("Lock file {} released".format(lock.lock_file))

    return 0


if __name__ == "__main__":
    main()
