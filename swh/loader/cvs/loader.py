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
import subprocess
import tempfile
import time
from typing import Dict, Iterator, List, Optional, Tuple
from urllib3.util import parse_url

from swh.loader.core.loader import BaseLoader
from swh.loader.core.utils import clean_dangling_folders
from swh.loader.exception import NotFound
import swh.loader.cvs.rcsparse as rcsparse
from swh.loader.cvs.cvs2gitdump.cvs2gitdump import CvsConv, RcsKeywords, CHANGESET_FUZZ_SEC, file_path
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
        cvsroot_path: Optional[str] = None,
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
        self.cvs_module_name = None
        self.cvs_module_path = None
        self.cvs_changesets = None
        self.rcs = RcsKeywords()
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
        self._load_status = "uneventful"
        self.visit_date = visit_date
        self.cvsroot_path = cvsroot_path
        self.start_from_scratch = start_from_scratch
        self.snapshot = None
        # state from previous visit
        self.latest_snapshot = None
        self.latest_revision = None

    def prepare_origin_visit(self):
        self.origin = Origin(url=self.origin_url if self.origin_url else self.cvsroot_url)

    def cleanup(self):
        self.log.info("cleanup")

    def fetch_cvs_repo_with_rsync(self, host, path_on_server):
        # URL *must* end with a trailing slash in order to get CVSROOT listed
        url = 'rsync://%s%s/' % (host, path_on_server)
        rsync = subprocess.run(['rsync', url], capture_output=True, encoding='ascii')
        rsync.check_returncode()
        have_cvsroot = False
        have_module = False
        for line in rsync.stdout.split('\n'):
            self.log.debug("rsync server: %s" % line)
            if line.endswith(' CVSROOT'):
                have_cvsroot = True
            elif line.endswith(' %s' % self.cvs_module_name):
                have_module = True
            if have_module and have_cvsroot:
                break
        if not have_module:
            raise NotFound("CVS module %s not found at %s" \
                % (self.cvs_module_name, host, url))
        if not have_cvsroot:
            raise NotFound("No CVSROOT directory found at %s" % url)

        rsync = subprocess.run(['rsync', '-a', url, self.cvsroot_path])
        rsync.check_returncode()

    def prepare(self):
        if not self.cvsroot_path:
            self.cvsroot_path = tempfile.mkdtemp(
                suffix="-%s" % os.getpid(),
                prefix=TEMPORARY_DIR_PREFIX_PATTERN,
                dir=self.temp_directory,
            )
        self.worktree_path = tempfile.mkdtemp(
            suffix="-%s" % os.getpid(),
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            dir=self.temp_directory,
        )
        url = parse_url(self.origin_url)
        self.log.debug("prepare; origin_url=%s scheme=%s path=%s" % (self.origin_url, url.scheme, url.path))
        if not url.path:
            raise NotFound("Invalid CVS origin URL '%s'" % self.origin_url)
        self.cvs_module_name = os.path.basename(url.path)
        os.mkdir(os.path.join(self.worktree_path, self.cvs_module_name));
        self.cvs_module_path = os.path.join(self.cvsroot_path, self.cvs_module_name)
        if url.scheme == 'file':
            if not os.path.exists(url.path):
                raise NotFound
        elif url.scheme == 'rsync':
            self.fetch_cvs_repo_with_rsync(url.host, url.path)
        else:
            raise NotFound("Invalid CVS origin URL '%s'" % self.origin_url)
        have_rcsfile = False
        have_cvsroot = False
        for root, dirs, files in os.walk(self.cvsroot_path):
            if 'CVSROOT' in dirs:
                have_cvsroot = True
                dirs.remove('CVSROOT')
                continue;
            for f in files:
                filepath = os.path.join(root, f)
                if f[-2:] == ',v':
                    try:
                      rcsfile = rcsparse.rcsfile(filepath)
                    except(Exception):
                        raise
                    else:
                        self.log.debug("Looks like we have data to convert; "
                            "found a valid RCS file at %s" % filepath)
                        have_rcsfile = True
                        break
            if have_rcsfile:
                break;

        if not have_rcsfile:
            raise NotFound("Directory %s does not contain any valid RCS files %s" % self.cvsroot_path)
        if not have_cvsroot:
            self.log.warn("The CVS repository at '%s' lacks a CVSROOT directory; "
                "we might be ingesting an incomplete copy of the repository" % self.cvsroot_path)

    def fetch_data(self):
        cvs = CvsConv(self.cvsroot_path, self.rcs, False, CHANGESET_FUZZ_SEC)
        self.log.debug("Walking CVS module %s", self.cvs_module_name)
        cvs.walk(self.cvs_module_name)
        self.cvs_changesets = sorted(cvs.changesets)
        self.log.info('CVS changesets found in %s: %d' % (self.cvs_module_name, len(self.cvs_changesets)))

    def store_data(self):
        for k in self.cvs_changesets:
            tstr = time.strftime('%c', time.gmtime(k.max_time))
            self.log.debug("changeset from %s by %s on branch %s", tstr, k.author, k.branch);
            for f in k.revs:
                path = file_path(self.cvsroot_path, f.path)
                wtpath = os.path.join(self.worktree_path, path)
                self.log.debug("rev %s of file %s" % (f.rev, f.path));
                if f.state == 'dead':
                    # remove this file from work tree
                    try:
                        os.remove(wtpath)
                    except FileNotFoundError:
                        pass
                else:
                    # create, or update, this file in the work tree
                    contents = self.rcs.expand_keyword(f.path, f.rev)
                    try:
                        outfile = open(wtpath, mode='wb')
                    except FileNotFoundError:
                        os.makedirs(os.path.dirname(wtpath))
                        outfile = open(wtpath, mode='wb')
                    outfile.write(contents)
                    outfile.close()

    def load_status(self):
        return {
            "status": self._load_status,
        }

    def visit_status(self):
        return self._visit_status

