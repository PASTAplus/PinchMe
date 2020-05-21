#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: pinchme

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


cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/pinchme.log"
daiquiri.setup(
    level=logging.INFO, outputs=(daiquiri.output.File(logfile), "stdout",)
)
logger = daiquiri.getLogger(__name__)


@click.command()
def main():
    return 0


if __name__ == "__main__":
    main()
