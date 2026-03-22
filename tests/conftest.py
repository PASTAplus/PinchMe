#!/usr/bin/env python

"""
:Mod: conftest

:Synopsis:
    Shared pytest fixtures for the PinchMe test suite.

:Author:
    servilla

:Created:
    3/19/26
"""

from pathlib import Path

import pytest

from pinchme.config import Config
from pinchme.model.resource_db import ResourcePool


@pytest.fixture()
def rp():
    """Return a ResourcePool instance connected to the test database."""
    resource_pool = ResourcePool(Config.PINCHME_TEST_DB)
    return resource_pool


@pytest.fixture()
def clean_up():
    """Remove the test database file after tests are complete."""
    yield
    Path(Config.PINCHME_TEST_DB).unlink(missing_ok=True)
