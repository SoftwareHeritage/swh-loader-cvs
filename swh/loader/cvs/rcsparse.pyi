# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections.abc import Mapping
from typing import Any, List, Tuple

def __getattr__(name) -> Any: ...

class rcsfile:
    head: str
    branch: str
    access: List[str]
    symbols: Mapping[str, str]  # actually rcsparse.rcstokmap
    locks: Mapping[str, str]  # actually rcsparse.rcstokmap
    strict: bool
    comment: str
    expand: str
    revs: Mapping[str, Tuple[str, int, str, str, List[str], str, str]] # actually rcsparse.rcsrevtree
    desc: str

    def __init__(self, path: bytes): ...

    def checkout(self, rev: str = "HEAD") -> bytes: ...
    def getlog(self, rev: str) -> bytes: ...
    def sym2rev(self, rev: str = "HEAD") -> str: ...
