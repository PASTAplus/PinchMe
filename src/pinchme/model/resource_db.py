#!/usr/bin/env python

"""
:Mod: resource_db

:Synopsis:
    The resource database model.

:Author:
    servilla

:Created:
    5/20/20
"""

from collections.abc import Sequence
from datetime import datetime

import daiquiri
from sqlalchemy import (
    Boolean,
    DateTime,
    Integer,
    String,
    asc,
    desc,
    func,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import not_

logger = daiquiri.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class Packages(Base):
    __tablename__ = "packages"

    id: Mapped[str] = mapped_column(String(), primary_key=True)
    date_created: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    validated: Mapped[bool] = mapped_column(Boolean(), nullable=False, default=False)


class Resources(Base):
    __tablename__ = "resources"

    id: Mapped[str] = mapped_column(String(), primary_key=True)
    pid: Mapped[str] = mapped_column(String(), nullable=False)
    type: Mapped[str] = mapped_column(String(), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(), nullable=True)
    md5: Mapped[str | None] = mapped_column(String(), nullable=True)
    sha1: Mapped[str | None] = mapped_column(String(), nullable=True)
    bytesize: Mapped[int | None] = mapped_column(Integer(), nullable=True)
    location: Mapped[str | None] = mapped_column(String(), nullable=True)
    checked_count: Mapped[int] = mapped_column(Integer(), nullable=False, default=0)
    checked_last_date: Mapped[datetime | None] = mapped_column(
        DateTime(), nullable=True
    )
    checked_last_status: Mapped[int | None] = mapped_column(Integer(), nullable=True)
    validated: Mapped[bool] = mapped_column(Boolean(), nullable=False, default=False)


class ResourcePool:
    def __init__(self, db: str):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///" + db)
        Base.metadata.create_all(engine)
        session_factory = sessionmaker(bind=engine)
        self.session = session_factory()

    def get_unvalidated_packages(
        self, col: str = "date_created", order: str = "asc"
    ) -> Sequence[Packages]:
        p = []

        if col == "id":
            column = Packages.id
        else:
            column = Packages.date_created

        try:
            stmt = select(Packages).filter(not_(Packages.validated))
            if order == "random":
                stmt = stmt.order_by(func.random())
            elif order == "desc":
                stmt = stmt.order_by(desc(column))
            else:  # order == "asc":
                stmt = stmt.order_by(asc(column))

            p = self.session.execute(stmt).scalars().all()

        except NoResultFound as e:
            logger.error(e)
        return p

    def get_packages(
        self, col: str = "date_created", order: str = "asc"
    ) -> Sequence[Packages]:
        p = []

        if col == "id":
            column = Packages.id
        else:
            column = Packages.date_created

        try:
            stmt = select(Packages)
            if order == "random":
                stmt = stmt.order_by(func.random())
            elif order == "desc":
                stmt = stmt.order_by(desc(column))
            else:  # order == "asc":
                stmt = stmt.order_by(asc(column))

            p = self.session.execute(stmt).scalars().all()

        except NoResultFound as e:
            logger.error(e)
        return p

    def get_last_package_create_date(self) -> datetime | None:
        d = None
        try:
            stmt = select(Packages).order_by(Packages.date_created.desc()).limit(1)
            p = self.session.execute(stmt).scalar_one_or_none()
            if p is not None:
                d = p.date_created
        except NoResultFound as e:
            logger.error(e)
        return d

    def get_package(self, id: str) -> Packages | None:
        p = None
        try:
            stmt = select(Packages).filter(Packages.id == id)
            p = self.session.execute(stmt).scalar_one_or_none()
        except NoResultFound as e:
            logger.error(e)
        return p

    def insert_package(self, id: str, date_created: datetime):
        p = Packages(id=id, date_created=date_created)
        try:
            self.session.add(p)
            self.session.commit()
            logger.info(f"Inserting package: {p.id}")
        except IntegrityError as e:
            logger.error(e)
            self.session.rollback()
            raise e

    def set_unvalidated_packages(self):
        try:
            stmt = update(Packages).values({Packages.validated: False})
            self.session.execute(stmt)
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e

    def set_validated_package(self, id: str):
        try:
            stmt = (
                update(Packages)
                .filter(Packages.id == id)
                .values({Packages.validated: True})
            )
            self.session.execute(stmt)
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e

    def get_failed_resources(self) -> Sequence[tuple[Packages, Resources]]:
        r = []
        try:
            stmt = (
                select(Packages, Resources)
                .join(Resources, Packages.id == Resources.pid)
                .filter(Resources.checked_last_status != 0)
            )
            r = self.session.execute(stmt).all()
        except NoResultFound as e:
            logger.error(e)
        return r

    def get_unvalidated_resources(self) -> Sequence[Resources]:
        r = []
        try:
            stmt = select(Resources).filter(not_(Resources.validated))
            r = self.session.execute(stmt).scalars().all()
        except NoResultFound as e:
            logger.error(e)
        return r

    def get_package_resources(self, pid: str) -> Sequence[Resources]:
        r = []
        try:
            stmt = select(Resources).filter(Resources.pid == pid)
            r = self.session.execute(stmt).scalars().all()
        except NoResultFound as e:
            logger.error(e)
        return r

    def get_resource(self, id: str) -> Resources | None:
        p = None
        try:
            stmt = select(Resources).filter(Resources.id == id)
            p = self.session.execute(stmt).scalar_one()
        except NoResultFound as e:
            logger.error(e)
        return p

    def insert_resource(
        self,
        id: str,
        pid: str,
        type: str,
        entity_id: str | None,
        md5: str | None,
        sha1: str | None,
        bytesize: int | None = None,
        location: str | None = None,
    ):
        r = Resources(
            id=id,
            pid=pid,
            type=type,
            entity_id=entity_id,
            md5=md5,
            sha1=sha1,
            bytesize=bytesize,
            location=location,
        )
        try:
            self.session.add(r)
            self.session.commit()
        except IntegrityError as e:
            logger.error(e)
            self.session.rollback()
            raise e

    def set_unvalidated_resources(self):
        try:
            stmt = update(Resources).values({Resources.validated: False})
            self.session.execute(stmt)
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e

    def set_validated_resource(self, id: str):
        try:
            stmt = (
                update(Resources)
                .filter(Resources.id == id)
                .values({Resources.validated: True})
            )
            self.session.execute(stmt)
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e

    def set_status_resource(self, id: str, count: int, date: datetime, status: int):
        try:
            stmt = (
                update(Resources)
                .filter(Resources.id == id)
                .values(
                    {
                        Resources.checked_count: count,
                        Resources.checked_last_date: date,
                        Resources.checked_last_status: status,
                    },
                )
            )
            self.session.execute(stmt)
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e

    def update_resource(
        self, id: str, md5: str | None, sha1: str | None, bytesize: int | None
    ):
        try:
            stmt = (
                update(Resources)
                .filter(Resources.id == id)
                .values(
                    {
                        Resources.md5: md5,
                        Resources.sha1: sha1,
                        Resources.bytesize: bytesize,
                    }
                )
            )
            self.session.execute(stmt)
            self.session.commit()
        except NoResultFound as e:
            logger.error(e)
            raise e
