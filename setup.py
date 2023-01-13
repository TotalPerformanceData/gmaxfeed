#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 21:41:11 2023

setuptools file for gmaxfeed to install into pip site-packages

@author: George
"""

from setuptools import setup, find_packages
import pathlib

path = pathlib.Path(__file__).parent.resolve()

long_description = (path / "README.md").read_text(encoding = "utf-8")

setup(
    name = "gmaxfeed",
    version = 0.1,
    description = "endpoints to interact with the Gmax API",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/TotalPerformanceData/gmaxfeed",
    author = "George Swindells",
    author_email = "george.swindells@totalperformancedata.com",
    packages = find_packages(
        exclude = ["logs", "tests", "rust-listener"]
        ),
    package_data = {
        '': ["*.txt"],
    },
    install_requires = [
        "python-dateutil",
        "numpy",
        "pandas",
        "requests",
        "bs4",
        "cryptography",
        "lxml"
        ]
    )

