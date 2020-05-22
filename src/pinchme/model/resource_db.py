#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: resource_db

:Synopsis:
    The resource database model.

:Author:
    servilla

:Created:
    5/20/20
"""
from datetime import datetime

import daiquiri
from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    desc,
    asc,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import not_

from pinchme.config import Config

logger = daiquiri.getLogger(__name__)

Base = declarative_base()


class Packages(Base):
    __tablename__ = "packages"

    id = Column(String(), primary_key=True)
    date_created = Column(DateTime(), nullable=False)
    dirty = Column(Boolean(), nullable=False, default=False)


class Resources(Base):
    __tablename__ = "resources"

    id = Column(String(), primary_key=True)
    pid = Column(String(), nullable=False)
    type = Column(String(), nullable=False)
    entity_id = Column(String(), nullable=True)
    md5 = Column(String(), nullable=False)
    sha1 = Column(String(), nullable=False)
    checked_count = Column(Integer(), nullable=False, default=0)
    checked_last_date = Column(DateTime(), nullable=True)
    checked_last_status = Column(Boolean(), nullable=True)
    dirty = Column(Boolean(), nullable=False, default=False)


class ResourcePool:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def get_clean_packages(self) -> list:
        p = None
        try:
            p = self.session.query(Packages).filter(not_(Packages.dirty)).all()
        except NoResultFound as e:
            logger.error(e)
        return p

    def get_last_package_create_date(self) -> datetime:
        d = None
        try:
            p = (
                self.session.query(Packages)
                .order_by(Packages.date_created.desc())
                .first()
            )
            if p is not None:
                d = p.date_created
        except NoResultFound as e:
            logger.error(e)
        return d

    def get_package(self, id: str) -> Packages:
        p = None
        try:
            p = self.session.query(Packages).filter(Packages.id == id).one()
        except NoResultFound as e:
            logger.error(e)
        return p

    def insert_package(self, id: str, date_created: datetime):
        p = Packages(id=id, date_created=date_created)
        try:
            self.session.add(p)
            self.session.commit()
        except IntegrityError as e:
            logger.error(e)
            self.session.rollback()
            raise e

    def set_clean_packages(self):
        try:
            p = self.session.query(Packages).update({Packages.dirty: False})
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e

    def set_dirty_package(self, id: str):
        try:
            p = (
                self.session.query(Packages)
                .filter(Packages.id == id)
                .update({Packages.dirty: True})
            )
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e

    def get_clean_resources(self) -> list:
        r = None
        try:
            r = (
                self.session.query(Resources)
                .filter(not_(Resources.dirty))
                .all()
            )
        except NoResultFound as e:
            logger.error(e)
        return r

    def get_package_resources(self, pid: str) -> list:
        r = None
        try:
            r = (
                self.session.query(Resources)
                .filter(Resources.pid == pid)
                .all()
            )
        except NoResultFound as e:
            logger.error(e)
        return r

    def get_resource(self, id: str) -> Resources:
        p = None
        try:
            p = self.session.query(Resources).filter(Resources.id == id).one()
        except NoResultFound as e:
            logger.error(e)
        return p

    def insert_resource(
        self, id: str, pid: str, type: str, entity_id: str, md5: str, sha1: str
    ):
        r = Resources(
            id=id, pid=pid, type=type, entity_id=entity_id, md5=md5, sha1=sha1,
        )
        try:
            self.session.add(r)
            self.session.commit()
        except IntegrityError as e:
            logger.error(e)
            self.session.rollback()
            raise e

    def set_clean_resources(self):
        try:
            r = self.session.query(Resources).update({Resources.dirty: False})
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e

    def set_dirty_resource(self, id: str):
        try:
            r = (
                self.session.query(Resources)
                .filter(Resources.id == id)
                .update({Resources.dirty: True})
            )
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e
