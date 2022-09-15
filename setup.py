#!/usr/bin/env python3
# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

from io import open
from os import path
import sys

from setuptools import Extension, find_packages, setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()


def parse_requirements(*names):
    requirements = []
    for name in names:
        if name:
            reqf = "requirements-%s.txt" % name
        else:
            reqf = "requirements.txt"

        if not path.exists(reqf):
            return requirements

        with open(reqf) as f:
            for line in f.readlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                requirements.append(line)
    return requirements


macros = []
if sys.version_info[:2] >= (3, 10):  # https://github.com/python/cpython/issues/85115
    macros.append(("PY_SSIZE_T_CLEAN", None))

setup(
    name="swh.loader.cvs",
    description="Software Heritage CVS Loader",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    python_requires=">=3.7",
    author="Software Heritage developers",
    author_email="swh-devel@inria.fr",
    url="https://forge.softwareheritage.org/diffusion/swh-loader-cvs",
    packages=find_packages(),  # packages's modules
    install_requires=parse_requirements(None, "swh"),
    tests_require=parse_requirements("test"),
    setup_requires=["setuptools-scm"],
    use_scm_version=True,
    extras_require={"testing": parse_requirements("test")},
    include_package_data=True,
    entry_points="""
        [swh.workers]
        loader.cvs=swh.loader.cvs:register
    """,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    project_urls={
        "Bug Reports": "https://forge.softwareheritage.org/maniphest",
        "Funding": "https://www.softwareheritage.org/donate",
        "Source": "https://forge.softwareheritage.org/source/swh-loader-cvs",
        "Documentation": "https://docs.softwareheritage.org/devel/swh-loader-cvs",
    },
    ext_modules=[
        Extension(
            "swh.loader.cvs.rcsparse",
            sources=[
                "swh/loader/cvs/rcsparse/py-rcsparse.c",
                "swh/loader/cvs/rcsparse/rcsparse.c",
            ],
            define_macros=macros,
        )
    ],
)
