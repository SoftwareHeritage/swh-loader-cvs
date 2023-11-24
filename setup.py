#!/usr/bin/env python3
# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

import sys

from setuptools import Extension, setup

macros = []
if sys.version_info[:2] >= (3, 10):  # https://github.com/python/cpython/issues/85115
    macros.append(("PY_SSIZE_T_CLEAN", None))

setup(
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
