# -*- coding: utf-8 -*-
"""setup.py for this module."""
import os
from codecs import open  # pylint: disable=W0622
from typing import List

from setuptools import setup, find_packages

import ecotricity_databricks_test as library

PATH_TO_REQUIREMENTS = os.path.abspath(os.path.join(os.path.dirname(__file__), "requirements.txt"))


def parse_requirements_txt(filename: str) -> List[str]:
    """Parse file with Python requirements (usually known as `requirements.txt`)."""
    with open(filename) as req_fd:
        req_file_lines = req_fd.read().splitlines()
        return [line for line in req_file_lines if bool(line.strip())]


with open("README.md", "r", "utf-8") as f:
    readme = f.read()


setup(
    name=library.__module__,
    version=library.__version__,
    author="Alexandre Gattiker",
    author_email="algattik@microsoft.com",
    description=library.__description__,
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/algattik/databricks_test",
    packages=find_packages(),
    install_requires=parse_requirements_txt(PATH_TO_REQUIREMENTS),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
