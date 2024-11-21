#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: setup.py

:Synopsis:

:Author:
    servilla

:Created:
    5/18/2020
"""
from pathlib import Path
from setuptools import find_packages, setup


here = Path(".").resolve()

with open(here / 'README.md', encoding='utf-8') as f:
    long_description = f.read()

with open(here / 'LICENSE', encoding='utf-8') as f:
    full_license = f.read()

setup(
    name="pinchme",
    version="2024.11.20",
    description="Perform PASTA resource integrity analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Mark Servilla",
    url="https://github.com/PASTAplus/PinchMe",
    license=full_license,
    packages=find_packages(where="src", include=["pinchme"]),
    include_package_data=True,
    exclude_package_data={"": ["settings.py, properties.py, config.py"],},
    package_dir={"": "src"},
    python_requires=" >= 3.11",
    install_requires=[
        "click >= 8.1.7",
        "daiquiri >= 3.0.0",
        "sqlalchemy >= 1.4.51",
        "pendulum >= 2.1.2"
    ],
    entry_points={"console_scripts": ["pinchme=pinchme.pinchy:main"]},
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
    ],
)


def main():
    return 0


if __name__ == "__main__":
    main()
