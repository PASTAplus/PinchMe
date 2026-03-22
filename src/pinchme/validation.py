#!/usr/bin/env python

"""
:Mod: validation

:Synopsis:

:Author:
    servilla

:Created:
    5/23/20
"""

import hashlib
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from time import sleep

import daiquiri

import pinchme.mimemail as mimemail
from pinchme.config import Config
from pinchme.model.resource_db import Packages, ResourcePool, Resources

logger = daiquiri.getLogger(__name__)


def integrity_check_packages(
    packages: Sequence[Packages] | None, delay: int, email: bool, verbose: int
):
    rp = ResourcePool(Config.PINCHME_DB)
    if packages is None:
        msg = "No data packages specified for validation"
        logger.warning(msg)
        return

    for package in packages:
        if verbose >= 1:
            print(f"Validating package '{package.id}'")
        resources = rp.get_package_resources(str(package.id))
        for resource in resources:
            if verbose >= 3:
                print(f"Validating resource '{resource.id}'")
            invalid = 0b0000
            pid = resource.pid
            invalid = invalid | valid_md5(resource)
            invalid = invalid | valid_sha1(resource)
            if resource.type == "data":
                invalid = invalid | valid_size(resource)
            date = datetime.now()
            count = resource.checked_count + 1
            rp.set_status_resource(str(resource.id), count, date, invalid)
            rp.set_validated_resource(str(resource.id))
            if invalid and email:
                subject = f"Integrity Error: {pid}"
                msg = f"Resource '{resource.id}' failed integrity check"
                mimemail.send_mail(subject, msg)
            sleep(delay)
        rp.set_validated_package(str(package.id))


def recheck_failed_resources(delay: int, email: bool, verbose: int):
    rp = ResourcePool(Config.PINCHME_DB)
    resources = rp.get_failed_resources()
    for _package, resource in resources:
        if verbose >= 1:
            print(f"Revalidating resource '{resource.id}'")
        invalid = 0b0000
        pid = resource.pid
        invalid = invalid | valid_md5(resource)
        invalid = invalid | valid_sha1(resource)
        if resource.type == "data":
            invalid = invalid | valid_size(resource)
        date = datetime.now()
        count = resource.checked_count + 1
        rp.set_status_resource(str(resource.id), count, date, invalid)
        rp.set_validated_resource(str(resource.id))
        if invalid and email:
            subject = f"Integrity Error: {pid}"
            msg = f"Resource '{resource.id}' failed integrity check"
            mimemail.send_mail(subject, msg)
        sleep(delay)


def show_failed_resources() -> Sequence[tuple[Packages, Resources]]:
    rp = ResourcePool(Config.PINCHME_DB)
    return rp.get_failed_resources()


def _resolve_resource_path(resource: Resources) -> Path:
    """Build the filesystem path for a resource based on its type."""
    base = Path(resource.location) / resource.pid
    if resource.type == "metadata":
        return base / "Level-1-EML.xml"
    elif resource.type == "report":
        return base / "quality_report.xml"
    else:  # resource.type == "data"
        return base / resource.entity_id


def valid_md5(resource: Resources) -> int:
    status = 0b0000
    resource_path = _resolve_resource_path(resource)

    if not resource_path.exists():
        logger.error(f"Resource '{resource_path}' not found")
        return 0b1000

    logger.info(f"Validating checksum for resource '{resource_path}'")
    with open(resource_path, "rb") as f:
        md5_hash = hashlib.md5()
        while chunk := f.read(8192):
            md5_hash.update(chunk)
        md5 = md5_hash.hexdigest()
    if md5 == resource.md5:
        logger.info(f"Resource '{resource_path}' is valid")
    else:
        status = 0b0001
        logger.error(
            f"Resource '{resource_path}' is not valid - "
            f"expected md5 '{resource.md5}', but got '{md5}'"
        )
    return status


def valid_sha1(resource: Resources) -> int:
    status = 0b0000
    resource_path = _resolve_resource_path(resource)

    if not resource_path.exists():
        logger.error(f"Resource '{resource_path}' not found")
        return 0b1000

    logger.info(f"Validating checksum for resource '{resource_path}'")
    with open(resource_path, "rb") as f:
        sha1_hash = hashlib.sha1()
        while chunk := f.read(8192):
            sha1_hash.update(chunk)
        sha1 = sha1_hash.hexdigest()
    if sha1 == resource.sha1:
        logger.info(f"Resource '{resource_path}' is valid")
    else:
        status = 0b0010
        logger.error(
            f"Resource '{resource_path}' is not valid - "
            f"expected sha1 '{resource.sha1}', but got '{sha1}'"
        )
    return status


def valid_size(resource: Resources) -> int:
    status = 0b0000
    resource_path = _resolve_resource_path(resource)

    if not resource_path.exists():
        logger.error(f"Resource '{resource_path}' not found")
        return 0b1000

    logger.info(f"Validating size for resource '{resource_path}'")
    size = resource_path.stat().st_size
    if size == resource.bytesize:
        logger.info(f"Resource '{resource_path}' is valid")
    else:
        status = 0b0100
        logger.error(
            f"Resource '{resource_path}' is not valid - "
            f"expected size '{resource.bytesize}', but got '{size}'"
        )
    return status
