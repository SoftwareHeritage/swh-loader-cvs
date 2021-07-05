# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting either new or existing cvs repositories to
swh-storage.

"""
from datetime import datetime
from mmap import ACCESS_WRITE, mmap
import os
import pty
import re
import shutil
from subprocess import Popen
import tempfile
from typing import Dict, Iterator, List, Optional, Tuple

from swh.loader.core.loader import BaseLoader
from swh.loader.core.utils import clean_dangling_folders
from swh.loader.exception import NotFound
from swh.model import from_disk, hashutil
from swh.model.model import (
    Content,
    Directory,
    Origin,
    Revision,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.storage.algos.snapshot import snapshot_get_latest
from swh.storage.interface import StorageInterface

DEFAULT_BRANCH = b"HEAD"

TEMPORARY_DIR_PREFIX_PATTERN = "swh.loader.cvs."


class CvsLoader(BaseLoader):
    """Swh cvs loader.

    The repository is local.  The loader deals with
    update on an already previously loaded repository.

    """

    visit_type = "cvs"

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        origin_url: Optional[str] = None,
        visit_date: Optional[datetime] = None,
        destination_path: Optional[str] = None,
        swh_revision: Optional[str] = None,
        start_from_scratch: bool = False,
        temp_directory: str = "/tmp",
        debug: bool = False,
        check_revision: int = 0,
        max_content_size: Optional[int] = None,
    ):
        super().__init__(
            storage=storage,
            logging_class="swh.loader.cvs.CvsLoader",
            max_content_size=max_content_size,
        )
        self.cvsroot_url = url
        # origin url as unique identifier for origin in swh archive
        self.origin_url = origin_url if origin_url else self.cvsroot_url
        self.debug = debug
        self.temp_directory = temp_directory
        self.done = False
        self.cvsrepo = None
        # Revision check is configurable
        self.check_revision = check_revision
        # internal state used to store swh objects
        self._contents: List[Content] = []
        self._skipped_contents: List[SkippedContent] = []
        self._directories: List[Directory] = []
        self._revisions: List[Revision] = []
        self._snapshot: Optional[Snapshot] = None
        # internal state, current visit
        self._last_revision = None
        self._visit_status = "full"
        self.visit_date = visit_date
        self.destination_path = destination_path
        self.start_from_scratch = start_from_scratch
        self.snapshot = None
        # state from previous visit
        self.latest_snapshot = None
        self.latest_revision = None

    def load_status(self):
        return {
            "status": self._load_status,
        }

    def visit_status(self):
        return self._visit_status

