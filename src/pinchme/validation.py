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
from time import sleep
import hashlib
from pathlib import Path

import daiquiri
from sqlalchemy.orm.query import Query

from pinchme.config import Config
import pinchme.mimemail as mimemail
from pinchme.model.resource_db import Resources, ResourcePool


logger = daiquiri.getLogger(__name__)


def integrity_check_packages(packages: Query, delay: int, email: bool):
    rp = ResourcePool(Config.PINCHME_DB)
    if packages is None:
        msg = "No data packages specified for validation"
        logger.warning(msg)
    for package in packages:
        resources = rp.get_package_resources(package.id)
        for resource in resources:
            invalid = 0b0000
            pid = resource.pid
            invalid = invalid | valid_md5(resource)
            invalid = invalid | valid_sha1(resource)
            if resource.type == "data":
                invalid = invalid | valid_size(resource)
            date = datetime.now()
            count = resource.checked_count + 1
            rp.set_status_resource(resource.id, count, date, invalid)
            rp.set_validated_resource(resource.id)
            if invalid and email:
                subject = f"Integrity Error: {pid}"
                msg = f"Resource '{resource.id}' failed integrity check"
                mimemail.send_mail(subject, msg)
            sleep(delay)
        rp.set_validated_package(package.id)


def recheck_failed_resources(delay: int, email: bool):
    rp = ResourcePool(Config.PINCHME_DB)
    resources = rp.get_failed_resources()
    for resource in resources:
        invalid = 0b0000
        pid = resource.pid
        invalid = invalid | valid_md5(resource)
        invalid = invalid | valid_sha1(resource)
        if resource.type == "data":
            invalid = invalid | valid_size(resource)
        date = datetime.now()
        count = resource.checked_count + 1
        rp.set_status_resource(resource.id, count, date, invalid)
        rp.set_validated_resource(resource.id)
        if invalid and email:
            subject = f"Integrity Error: {pid}"
            msg = f"Resource '{resource.id}' failed integrity check"
            mimemail.send_mail(subject, msg)
        sleep(delay)


def show_failed_resources() -> Query:
    rp = ResourcePool(Config.PINCHME_DB)
    return rp.get_failed_resources()


def valid_md5(resource: Resources) -> int:
    status = 0b000
    if resource.type == "metadata":
        path_head = f"{Config.METADATA_STORE}/{resource.pid}"
        resource_path = f"{path_head}/Level-1-EML.xml"
    elif resource.type == "report":
        path_head = f"{Config.METADATA_STORE}/{resource.pid}"
        resource_path = f"{path_head}/quality_report.xml"
    else:  # resource.type == "data"
        path_head = f"{Config.DATA_STORE}/{resource.pid}"
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
            msg = f"Resource '{resource_path}' is valid"
            logger.info(msg)
        else:
            status = 0b0001
            msg = (
                f"Resource '{resource_path}' is not valid -  "
                f"expected md5 '{resource.md5}', but got '{md5}'"
            )
            logger.error(msg)
    else:
        status = 0b1000
        msg = f"Resource '{resource_path}' not found"
        logger.error(msg)
    return status


def valid_sha1(resource: Resources) -> int:
    status = 0b0000
    if resource.type == "metadata":
        path_head = f"{Config.METADATA_STORE}/{resource.pid}"
        resource_path = f"{path_head}/Level-1-EML.xml"
    elif resource.type == "report":
        path_head = f"{Config.METADATA_STORE}/{resource.pid}"
        resource_path = f"{path_head}/quality_report.xml"
    else:  # resource.type == "data"
        path_head = f"{Config.DATA_STORE}/{resource.pid}"
        resource_path = f"{path_head}/{resource.entity_id}"

    if Path(resource_path).exists():
        msg = f"Validating checksum for resource '{resource_path}'"
        logger.info(msg)
        with open(resource_path, "rb") as f:
            sha1_hash = hashlib.sha1()
            while chunk := f.read(8192):
                sha1_hash.update(chunk)
            sha1 = sha1_hash.hexdigest()
        if sha1 == resource.sha1:
            msg = f"Resource '{resource_path}' is valid"
            logger.info(msg)
        else:
            status = 0b0010
            msg = (
                f"Resource '{resource_path}' is not valid -  "
                f"expected sha1 '{resource.sha1}', but got '{sha1}'"
            )
            logger.error(msg)
    else:
        status = 0b1000
        msg = f"Resource '{resource_path}' not found"
        logger.error(msg)
    return status


def valid_size(resource: Resources) -> int:
    status = 0b0000
    if resource.type == "metadata":
        path_head = f"{Config.METADATA_STORE}/{resource.pid}"
        resource_path = f"{path_head}/Level-1-EML.xml"
    elif resource.type == "report":
        path_head = f"{Config.METADATA_STORE}/{resource.pid}"
        resource_path = f"{path_head}/quality_report.xml"
    else:  # resource.type == "data"
        path_head = f"{Config.DATA_STORE}/{resource.pid}"
        resource_path = f"{path_head}/{resource.entity_id}"

    if Path(resource_path).exists():
        msg = f"Validating size for resource '{resource_path}'"
        logger.info(msg)
        size = Path(resource_path).stat().st_size
        if size == resource.bytesize:
            msg = f"Resource '{resource_path}' is valid"
            logger.info(msg)
        else:
            status = 0b0100
            msg = (
                f"Resource '{resource_path}' is not valid -  "
                f"expected size '{resource.bytesize}', but got '{size}'"
            )
            logger.error(msg)
    else:
        status = 0b1000
        msg = f"Resource '{resource_path}' not found"
        logger.error(msg)
    return status