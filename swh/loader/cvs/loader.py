# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting either new or existing cvs repositories to
swh-storage.

"""
from datetime import datetime
import os
import subprocess
import tempfile
import time
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Sequence, Tuple

from urllib3.util import parse_url

from swh.loader.core.loader import BaseLoader
from swh.loader.core.utils import clean_dangling_folders
from swh.loader.cvs.cvs2gitdump.cvs2gitdump import (
    CHANGESET_FUZZ_SEC,
    ChangeSetKey,
    CvsConv,
    FileRevision,
    RcsKeywords,
    file_path,
)
from swh.loader.cvs.cvsclient import CVSClient
import swh.loader.cvs.rcsparse as rcsparse
from swh.loader.cvs.rlog import RlogConv
from swh.loader.exception import NotFound
from swh.model import from_disk, hashutil
from swh.model.model import (
    Content,
    Directory,
    Origin,
    Person,
    Revision,
    RevisionType,
    Sha1Git,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    TargetType,
    TimestampWithTimezone,
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

    cvs_module_name: str
    cvsclient: CVSClient

    # remote CVS repository access (history is parsed from CVS rlog):
    rlog_file: BinaryIO

    swh_revision_gen: Iterator[
        Tuple[List[Content], List[SkippedContent], List[Directory], Revision]
    ]

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        origin_url: Optional[str] = None,
        visit_date: Optional[datetime] = None,
        cvsroot_path: Optional[str] = None,
        temp_directory: str = "/tmp",
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
        self.temp_directory = temp_directory

        # internal state used to store swh objects
        self._contents: List[Content] = []
        self._skipped_contents: List[SkippedContent] = []
        self._directories: List[Directory] = []
        self._revisions: List[Revision] = []
        # internal state, current visit
        self._last_revision: Optional[Revision] = None
        self._visit_status = "full"
        self.visit_date = visit_date

        if not cvsroot_path:
            cvsroot_path = tempfile.mkdtemp(
                suffix="-%s" % os.getpid(),
                prefix=TEMPORARY_DIR_PREFIX_PATTERN,
                dir=self.temp_directory,
            )
        self.cvsroot_path = cvsroot_path

        self.snapshot: Optional[Snapshot] = None
        self.last_snapshot: Optional[Snapshot] = snapshot_get_latest(
            self.storage, self.origin_url
        )

    def compute_swh_revision(
        self, k: ChangeSetKey, logmsg: Optional[bytes]
    ) -> Tuple[Revision, from_disk.Directory]:
        """Compute swh hash data per CVS changeset.

        Returns:
            tuple (rev, swh_directory)
            - rev: current SWH revision computed from checked out work tree
            - swh_directory: dictionary of path, swh hash data with type

        """
        # Compute SWH revision from the on-disk state
        swh_dir = from_disk.Directory.from_disk(path=os.fsencode(self.worktree_path))
        parents: Tuple[Sha1Git, ...]
        if self._last_revision:
            parents = (self._last_revision.id,)
        else:
            parents = ()
        revision = self.build_swh_revision(k, logmsg, swh_dir.hash, parents)
        self.log.debug("SWH revision ID: %s", hashutil.hash_to_hex(revision.id))
        self._last_revision = revision
        return (revision, swh_dir)

    def checkout_file_with_rcsparse(
        self, k: ChangeSetKey, f: FileRevision, rcsfile: rcsparse.rcsfile
    ) -> None:
        path = file_path(self.cvsroot_path, f.path)
        wtpath = os.path.join(self.worktree_path, path)
        self.log.info("rev %s state %s file %s" % (f.rev, f.state, f.path))
        if f.state == "dead":
            # remove this file from work tree
            try:
                os.remove(wtpath)
            except FileNotFoundError:
                pass
        else:
            # create, or update, this file in the work tree
            if not rcsfile:
                rcsfile = rcsparse.rcsfile(f.path)
            rcs = RcsKeywords()
            contents = rcs.expand_keyword(f.path, rcsfile, f.rev)
            os.makedirs(os.path.dirname(wtpath), exist_ok=True)
            outfile = open(wtpath, mode="wb")
            outfile.write(contents)
            outfile.close()

    def checkout_file_with_cvsclient(
        self, k: ChangeSetKey, f: FileRevision, cvsclient: CVSClient
    ):
        path = file_path(self.cvsroot_path, f.path)
        wtpath = os.path.join(self.worktree_path, path)
        self.log.info("rev %s state %s file %s" % (f.rev, f.state, f.path))
        if f.state == "dead":
            # remove this file from work tree
            try:
                os.remove(wtpath)
            except FileNotFoundError:
                pass
        else:
            dirname = os.path.dirname(wtpath)
            os.makedirs(dirname, exist_ok=True)
            self.log.debug("checkout to %s\n" % wtpath)
            fp = cvsclient.checkout(f.path, f.rev, dirname, expand_keywords=True)
            os.rename(fp.name, wtpath)
            try:
                fp.close()
            except FileNotFoundError:
                # Well, we have just renamed the file...
                pass

    def process_cvs_changesets(
        self, cvs_changesets: List[ChangeSetKey], use_rcsparse: bool,
    ) -> Iterator[
        Tuple[List[Content], List[SkippedContent], List[Directory], Revision]
    ]:
        """Process CVS revisions.

        At each CVS revision, check out contents and compute swh hashes.

        Yields:
            tuple (contents, skipped-contents, directories, revision) of dict as a
            dictionary with keys, sha1_git, sha1, etc...

        """
        for k in cvs_changesets:
            tstr = time.strftime("%c", time.gmtime(k.max_time))
            self.log.info(
                "changeset from %s by %s on branch %s", tstr, k.author, k.branch
            )
            logmsg: Optional[bytes] = b""
            # Check out all files of this revision and get a log message.
            #
            # The log message is obtained from the first file in the changeset.
            # The message will usually be the same for all affected files, and
            # the SWH archive will only store one version of the log message.
            for f in k.revs:
                rcsfile = None
                if use_rcsparse:
                    if rcsfile is None:
                        rcsfile = rcsparse.rcsfile(f.path)
                    if not logmsg:
                        logmsg = rcsfile.getlog(k.revs[0].rev)
                    self.checkout_file_with_rcsparse(k, f, rcsfile)
                else:
                    if not logmsg:
                        logmsg = self.rlog.getlog(self.rlog_file, f.path, k.revs[0].rev)
                    self.checkout_file_with_cvsclient(k, f, self.cvsclient)

            # TODO: prune empty directories?
            (revision, swh_dir) = self.compute_swh_revision(k, logmsg)
            (contents, skipped_contents, directories) = from_disk.iter_directory(
                swh_dir
            )
            yield contents, skipped_contents, directories, revision

    def prepare_origin_visit(self) -> None:
        self.origin = Origin(
            url=self.origin_url if self.origin_url else self.cvsroot_url
        )

    def pre_cleanup(self) -> None:
        """Cleanup potential dangling files from prior runs (e.g. OOM killed
        tasks)

        """
        clean_dangling_folders(
            self.temp_directory,
            pattern_check=TEMPORARY_DIR_PREFIX_PATTERN,
            log=self.log,
        )

    def cleanup(self) -> None:
        self.log.info("cleanup")

    def fetch_cvs_repo_with_rsync(self, host: str, path: str) -> None:
        # URL *must* end with a trailing slash in order to get CVSROOT listed
        url = "rsync://%s%s/" % (host, os.path.dirname(path))
        rsync = subprocess.run(["rsync", url], capture_output=True, encoding="ascii")
        rsync.check_returncode()
        have_cvsroot = False
        have_module = False
        for line in rsync.stdout.split("\n"):
            self.log.debug("rsync server: %s", line)
            if line.endswith(" CVSROOT"):
                have_cvsroot = True
            elif line.endswith(" %s" % self.cvs_module_name):
                have_module = True
            if have_module and have_cvsroot:
                break
        if not have_module:
            raise NotFound(
                "CVS module %s not found at %s" % (self.cvs_module_name, url)
            )
        if not have_cvsroot:
            raise NotFound("No CVSROOT directory found at %s" % url)

        subprocess.run(["rsync", "-a", url, self.cvsroot_path]).check_returncode()

    def prepare(self) -> None:
        self._last_revision = None
        self.worktree_path = tempfile.mkdtemp(
            suffix="-%s" % os.getpid(),
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            dir=self.temp_directory,
        )
        url = parse_url(self.origin_url)
        self.log.debug(
            "prepare; origin_url=%s scheme=%s path=%s",
            self.origin_url,
            url.scheme,
            url.path,
        )
        if not url.path:
            raise NotFound("Invalid CVS origin URL '%s'" % self.origin_url)
        self.cvs_module_name = os.path.basename(url.path)
        os.mkdir(os.path.join(self.worktree_path, self.cvs_module_name))
        if url.scheme == "file":
            if not os.path.exists(url.path):
                raise NotFound
        elif url.scheme == "rsync":
            self.fetch_cvs_repo_with_rsync(url.host, url.path)

        if url.scheme == "file" or url.scheme == "rsync":
            # local CVS repository conversion
            have_rcsfile = False
            have_cvsroot = False
            for root, dirs, files in os.walk(self.cvsroot_path):
                if "CVSROOT" in dirs:
                    have_cvsroot = True
                    dirs.remove("CVSROOT")
                    continue
                for f in files:
                    filepath = os.path.join(root, f)
                    if f[-2:] == ",v":
                        rcsfile = rcsparse.rcsfile(filepath)  # noqa: F841
                        self.log.debug(
                            "Looks like we have data to convert; "
                            "found a valid RCS file at %s",
                            filepath,
                        )
                        have_rcsfile = True
                        break
                if have_rcsfile:
                    break

            if not have_rcsfile:
                raise NotFound(
                    "Directory %s does not contain any valid RCS files %s",
                    self.cvsroot_path,
                )
            if not have_cvsroot:
                self.log.warn(
                    "The CVS repository at '%s' lacks a CVSROOT directory; "
                    "we might be ingesting an incomplete copy of the repository",
                    self.cvsroot_path,
                )

            # Unfortunately, there is no way to convert CVS history in an
            # iterative fashion because the data is not indexed by any kind
            # of changeset ID. We need to walk the history of each and every
            # RCS file in the repository during every visit, even if no new
            # changes will be added to the SWH archive afterwards.
            # "CVS’s repository is the software equivalent of a telephone book
            # sorted by telephone number."
            # https://corecursive.com/software-that-doesnt-suck-with-jim-blandy/
            #
            # An implicit assumption made here is that self.cvs_changesets will
            # fit into memory in its entirety. If it won't fit then the CVS walker
            # will need to be modified such that it spools the list of changesets
            # to disk instead.
            cvs = CvsConv(self.cvsroot_path, RcsKeywords(), False, CHANGESET_FUZZ_SEC)
            self.log.info("Walking CVS module %s", self.cvs_module_name)
            cvs.walk(self.cvs_module_name)
            cvs_changesets = sorted(cvs.changesets)
            self.log.info(
                "CVS changesets found in %s: %d",
                self.cvs_module_name,
                len(cvs_changesets),
            )
            self.swh_revision_gen = self.process_cvs_changesets(
                cvs_changesets, use_rcsparse=True
            )
        elif url.scheme == "pserver" or url.scheme == "fake" or url.scheme == "ssh":
            # remote CVS repository conversion
            self.cvsclient = CVSClient(url)
            cvsroot_path = os.path.dirname(url.path)
            self.log.info(
                "Fetching CVS rlog from %s:%s/%s",
                url.host,
                cvsroot_path,
                self.cvs_module_name,
            )
            self.rlog = RlogConv(cvsroot_path, CHANGESET_FUZZ_SEC)
            self.rlog_file = self.cvsclient.fetch_rlog()
            self.rlog.parse_rlog(self.rlog_file)
            cvs_changesets = sorted(self.rlog.changesets)
            self.log.info(
                "CVS changesets found for %s: %d",
                self.cvs_module_name,
                len(cvs_changesets),
            )
            self.swh_revision_gen = self.process_cvs_changesets(
                cvs_changesets, use_rcsparse=False
            )
        else:
            raise NotFound("Invalid CVS origin URL '%s'" % self.origin_url)

    def fetch_data(self) -> bool:
        """Fetch the next CVS revision."""
        try:
            data = next(self.swh_revision_gen)
        except StopIteration:
            return False
        except Exception:
            self.log.exception("Exception in fetch_data:")
            return False  # Stopping iteration
        self._contents, self._skipped_contents, self._directories, rev = data
        self._revisions = [rev]
        return True

    def build_swh_revision(
        self,
        k: ChangeSetKey,
        logmsg: Optional[bytes],
        dir_id: bytes,
        parents: Sequence[bytes],
    ) -> Revision:
        """Given a CVS revision, build a swh revision.

        Args:
            k: changeset data
            logmsg: the changeset's log message
            dir_id: the tree's hash identifier
            parents: the revision's parents identifier

        Returns:
            The swh revision dictionary.

        """
        author = Person.from_fullname(k.author.encode("UTF-8"))
        date = TimestampWithTimezone.from_dict(k.max_time)

        return Revision(
            type=RevisionType.CVS,
            date=date,
            committer_date=date,
            directory=dir_id,
            message=logmsg,
            author=author,
            committer=author,
            synthetic=True,
            extra_headers=[],
            parents=tuple(parents),
        )

    def generate_and_load_snapshot(self, revision: Revision) -> Snapshot:
        """Create the snapshot either from existing revision.

        Args:
            revision (dict): Last revision seen if any (None by default)

        Returns:
            Optional[Snapshot] The newly created snapshot

        """
        snap = Snapshot(
            branches={
                DEFAULT_BRANCH: SnapshotBranch(
                    target=revision.id, target_type=TargetType.REVISION
                )
            }
        )
        self.log.debug("snapshot: %s", snap)
        self.storage.snapshot_add([snap])
        return snap

    def store_data(self) -> None:
        "Add our current CVS changeset to the archive."
        self.storage.skipped_content_add(self._skipped_contents)
        self.storage.content_add(self._contents)
        self.storage.directory_add(self._directories)
        self.storage.revision_add(self._revisions)
        assert self._last_revision is not None
        self.snapshot = self.generate_and_load_snapshot(self._last_revision)
        self.log.debug("SWH snapshot ID: %s", hashutil.hash_to_hex(self.snapshot.id))
        self.flush()
        self.loaded_snapshot_id = self.snapshot.id
        self._skipped_contents = []
        self._contents = []
        self._directories = []
        self._revisions = []

    def load_status(self) -> Dict[str, Any]:
        assert self.snapshot is not None
        if self.last_snapshot == self.snapshot:
            load_status = "uneventful"
        else:
            load_status = "eventful"
        return {
            "status": load_status,
        }

    def visit_status(self) -> str:
        return self._visit_status
