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
from sqlalchemy import Boolean, Column, Integer, String, DateTime, ForeignKey, desc, asc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from pinchme.config import Config


logger = daiquiri.getLogger(__name__)

Base = declarative_base()


class Packages(Base):
    __tablename__ = "packages"

    id = Column(String(), primary_key=True)
    date_created = Column(DateTime(), nullable=False)


class Resources(Base):
    __tablename__ = "resources"

    id = Column(String(), primary_key=True)
    md5 = Column(String(), nullable=False)
    sha1 = Column(String(), nullable=False)
    checked_count = Column(Integer(), nullable=False, default=0)
    checked_last_date = Column(DateTime(), nullable=True)
    checked_last_status = Column(Boolean(), nullable=True)


class ResourcePool:

    def __init__(self, db: str):
        from sqlalchemy import create_engine
        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def get_package(self, id: str) -> Packages:
        p = None
        try:
            p = self.session.query(Packages).filter(
                    Packages.id == id).one()
        except NoResultFound as e:
            logger.error(e)
        return p

    def get_last_package_create_date(self) -> datetime:
        p = None
        try:
            p = self.session.query(Packages).order_by(
                Packages.date_created.desc()
            ).first()
        except NoResultFound as e:
            logger.error(e)
        return p.date_created

    def insert_package(self, id: str, date_created: datetime):
        p = Packages(
            id=id,
            date_created=date_created
        )
        try:
            self.session.add(p)
            self.session.commit()
        except IntegrityError as e:
            logger.error(e)
            self.session.rollback()
            raise e

    def insert_resource(self, id: str, md5: str, sha1: str):
        r = Resources(
            id=id,
            md5=md5,
            sha1=sha1,
        )
        try:
            self.session.add(r)
            self.session.commit()
        except IntegrityError as e:
            logger.error(e)
            self.session.rollback()
            raise e

    def get_resource(self, id: str) -> Resources:
        p = None
        try:
            p = self.session.query(Resources).filter(
                    Resources.id == id).one()
        except NoResultFound as e:
            logger.error(e)
        return p

