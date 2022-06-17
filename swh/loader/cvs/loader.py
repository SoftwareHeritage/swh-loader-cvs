# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting either new or existing cvs repositories to
swh-storage.

"""
from datetime import datetime
import os
import os.path
import subprocess
import tempfile
import time
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Sequence, Tuple, cast
from urllib.parse import urlparse

import sentry_sdk
from tenacity import retry
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt

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


def rsync_retry():
    return retry(
        retry=retry_if_exception_type(subprocess.CalledProcessError),
        stop=stop_after_attempt(max_attempt_number=4),
        reraise=True,
    )


class BadPathException(Exception):
    pass


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
        **kwargs: Any,
    ):
        self.cvsroot_url = url
        # origin url as unique identifier for origin in swh archive
        origin_url = origin_url if origin_url else self.cvsroot_url
        super().__init__(storage=storage, origin_url=origin_url, **kwargs)
        self.temp_directory = temp_directory

        # internal state used to store swh objects
        self._contents: List[Content] = []
        self._skipped_contents: List[SkippedContent] = []
        self._directories: List[Directory] = []
        self._revisions: List[Revision] = []
        # internal state, current visit
        self._last_revision: Optional[Revision] = None
        self._visit_status = "full"
        self.visit_date = visit_date or self.visit_date
        self.cvsroot_path = cvsroot_path
        self.custom_id_keyword = None
        self.excluded_keywords: List[str] = []

        self.snapshot: Optional[Snapshot] = None
        self.last_snapshot: Optional[Snapshot] = snapshot_get_latest(
            self.storage, self.origin.url
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

    def file_path_is_safe(self, wtpath):
        if "%s..%s" % (os.path.sep, os.path.sep) in wtpath:
            # Paths with back-references should not appear
            # in CVS protocol messages or CVS rlog output
            return False
        elif (
            os.path.commonpath([self.tempdir_path, os.path.normpath(wtpath)])
            != self.tempdir_path
        ):
            # The path must be a child of our temporary directory.
            return False
        else:
            return True

    def checkout_file_with_rcsparse(
        self, k: ChangeSetKey, f: FileRevision, rcsfile: rcsparse.rcsfile
    ) -> None:
        assert self.cvsroot_path
        assert self.server_style_cvsroot
        path = file_path(self.cvsroot_path, f.path)
        wtpath = os.path.join(self.tempdir_path, path)
        if not self.file_path_is_safe(wtpath):
            raise BadPathException(f"unsafe path found in RCS file: {f.path}")
        self.log.debug("rev %s state %s file %s", f.rev, f.state, f.path)
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

            # We try our best to generate the same commit hashes over both pserver
            # and rsync. To avoid differences in file content due to expansion of
            # RCS keywords which contain absolute file paths (such as "Header"),
            # attempt to expand such paths in the same way as a regular CVS server
            # would expand them.
            # Whether this will avoid content differences depends on pserver and
            # rsync servers exposing the same server-side path to the CVS repository.
            # However, this is the best we can do, and only matters if an origin can
            # be fetched over both pserver and rsync. Each will still be treated as
            # a distinct origin, but will hopefully point at the same SWH snapshot.
            # In any case, an absolute path based on the origin URL looks nicer than
            # an absolute path based on a temporary directory used by the CVS loader.
            server_style_path = f.path.replace(
                self.cvsroot_path, self.server_style_cvsroot
            )
            if server_style_path[0] != "/":
                server_style_path = "/" + server_style_path

            if self.custom_id_keyword is not None:
                rcs.add_id_keyword(self.custom_id_keyword)
            contents = rcs.expand_keyword(
                server_style_path, rcsfile, f.rev, self.excluded_keywords
            )
            os.makedirs(os.path.dirname(wtpath), exist_ok=True)
            outfile = open(wtpath, mode="wb")
            outfile.write(contents)
            outfile.close()

    def checkout_file_with_cvsclient(
        self, k: ChangeSetKey, f: FileRevision, cvsclient: CVSClient
    ):
        assert self.cvsroot_path
        path = file_path(self.cvsroot_path, f.path)
        wtpath = os.path.join(self.tempdir_path, path)
        if not self.file_path_is_safe(wtpath):
            raise BadPathException(f"unsafe path found in cvs rlog output: {f.path}")
        self.log.debug("rev %s state %s file %s", f.rev, f.state, f.path)
        if f.state == "dead":
            # remove this file from work tree
            try:
                os.remove(wtpath)
            except FileNotFoundError:
                pass
        else:
            dirname = os.path.dirname(wtpath)
            os.makedirs(dirname, exist_ok=True)
            self.log.debug("checkout to %s\n", wtpath)
            fp = cvsclient.checkout(path, f.rev, dirname, expand_keywords=True)
            os.rename(fp.name, wtpath)
            try:
                fp.close()
            except FileNotFoundError:
                # Well, we have just renamed the file...
                pass

    def process_cvs_changesets(
        self,
        cvs_changesets: List[ChangeSetKey],
        use_rcsparse: bool,
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
            self.log.debug(
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
        self.log.debug("cleanup")

    def configure_custom_id_keyword(self, cvsconfig):
        """Parse CVSROOT/config and look for a custom keyword definition.
        There are two different configuration directives in use for this purpose.

        The first variant stems from a patch which was never accepted into
        upstream CVS and uses the tag directive: tag=MyName
        With this, the "MyName" keyword becomes an alias for the "Id" keyword.
        This variant is prelevant in CVS versions shipped on BSD.

        The second variant stems from upstream CVS 1.12 and looks like:
        LocalKeyword=MyName=SomeKeyword
        KeywordExpand=iMyName
        We only support "SomeKeyword" if it specifies "Id" or "CVSHeader", for now.
        The KeywordExpand directive can be used to suppress expansion of keywords
        by listing keywords after an initial "e" character ("exclude", as opposed
        to an "include" list which uses an initial "i" character).
        For example, this disables expansion of the Date and Name keywords:
        KeywordExpand=eDate,Name
        """
        for line in cvsconfig.readlines():
            line = line.strip()
            try:
                (config_key, value) = line.split("=", 1)
            except ValueError:
                continue
            config_key = config_key.strip()
            value = value.strip()
            if config_key == "tag":
                self.custom_id_keyword = value
            elif config_key == "LocalKeyword":
                try:
                    (custom_kwname, kwname) = value.split("=", 1)
                except ValueError:
                    continue
                if kwname.strip() in ("Id", "CVSHeader"):
                    self.custom_id_keyword = custom_kwname.strip()
            elif config_key == "KeywordExpand" and value.startswith("e"):
                excluded_keywords = value[1:].split(",")
                for k in excluded_keywords:
                    self.excluded_keywords.append(k.strip())

    @rsync_retry()
    def execute_rsync(
        self, rsync_cmd: List[str], **run_opts
    ) -> subprocess.CompletedProcess:
        rsync = subprocess.run(rsync_cmd, **run_opts)
        rsync.check_returncode()
        return rsync

    def fetch_cvs_repo_with_rsync(self, host: str, path: str) -> None:
        # URL *must* end with a trailing slash in order to get CVSROOT listed
        url = "rsync://%s%s/" % (host, os.path.dirname(path))
        rsync = self.execute_rsync(
            ["rsync", url], capture_output=True, encoding="ascii"
        )
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
            raise NotFound(f"CVS module {self.cvs_module_name} not found at {url}")
        if not have_cvsroot:
            raise NotFound(f"No CVSROOT directory found at {url}")

        # Fetch the CVSROOT directory and the desired CVS module.
        assert self.cvsroot_path
        for d in ("CVSROOT", self.cvs_module_name):
            target_dir = os.path.join(self.cvsroot_path, d)
            os.makedirs(target_dir, exist_ok=True)
            # Append trailing path separators ("/" in the URL and os.path.sep in the
            # local target directory path) to ensure that rsync will place files
            # directly within our target directory .
            self.execute_rsync(
                ["rsync", "-az", url + d + "/", target_dir + os.path.sep]
            )

    def prepare(self) -> None:
        self._last_revision = None
        self.tempdir_path = tempfile.mkdtemp(
            suffix="-%s" % os.getpid(),
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            dir=self.temp_directory,
        )
        url = urlparse(self.origin.url)
        self.log.debug(
            "prepare; origin_url=%s scheme=%s path=%s",
            self.origin.url,
            url.scheme,
            url.path,
        )
        if not url.path:
            raise NotFound(f"Invalid CVS origin URL '{self.origin.url}'")
        self.cvs_module_name = os.path.basename(url.path)
        self.server_style_cvsroot = os.path.dirname(url.path)
        self.worktree_path = os.path.join(self.tempdir_path, self.cvs_module_name)
        if url.scheme == "file" or url.scheme == "rsync":
            # local CVS repository conversion
            if not self.cvsroot_path:
                self.cvsroot_path = tempfile.mkdtemp(
                    suffix="-%s" % os.getpid(),
                    prefix=TEMPORARY_DIR_PREFIX_PATTERN,
                    dir=self.temp_directory,
                )
            if url.scheme == "file":
                if not os.path.exists(url.path):
                    raise NotFound
            elif url.scheme == "rsync":
                assert url.hostname is not None
                self.fetch_cvs_repo_with_rsync(url.hostname, url.path)

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
                    f"Directory {self.cvsroot_path} does not contain any valid "
                    "RCS files",
                )
            if not have_cvsroot:
                self.log.warn(
                    "The CVS repository at '%s' lacks a CVSROOT directory; "
                    "we might be ingesting an incomplete copy of the repository",
                    self.cvsroot_path,
                )

            # The file CVSROOT/config will usually contain ASCII data only.
            # We allow UTF-8 just in case. Other encodings may result in an
            # error and will require manual intervention, for now.
            cvsconfig_path = os.path.join(self.cvsroot_path, "CVSROOT", "config")
            cvsconfig = open(cvsconfig_path, mode="r", encoding="utf-8")
            self.configure_custom_id_keyword(cvsconfig)
            cvsconfig.close()

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
            self.log.debug("Walking CVS module %s", self.cvs_module_name)
            cvs.walk(self.cvs_module_name)
            cvs_changesets = sorted(cvs.changesets)
            self.log.debug(
                "CVS changesets found in %s: %d",
                self.cvs_module_name,
                len(cvs_changesets),
            )
            self.swh_revision_gen = self.process_cvs_changesets(
                cvs_changesets, use_rcsparse=True
            )
        elif url.scheme == "pserver" or url.scheme == "fake" or url.scheme == "ssh":
            # remote CVS repository conversion
            if not self.cvsroot_path:
                self.cvsroot_path = os.path.dirname(url.path)
            self.cvsclient = CVSClient(url)
            cvsroot_path = os.path.dirname(url.path)
            self.log.debug(
                "Fetching CVS rlog from %s:%s/%s",
                url.hostname,
                cvsroot_path,
                self.cvs_module_name,
            )
            self.rlog = RlogConv(cvsroot_path, CHANGESET_FUZZ_SEC)
            main_rlog_file = self.cvsclient.fetch_rlog()
            self.rlog.parse_rlog(main_rlog_file)
            # Find file deletion events only visible in Attic directories.
            main_changesets = self.rlog.changesets
            attic_paths = []
            attic_rlog_files = []
            assert self.cvsroot_path
            for k in main_changesets:
                for changed_file in k.revs:
                    path = file_path(self.cvsroot_path, changed_file.path)
                    if path.startswith(self.cvsroot_path):
                        path = path[
                            len(os.path.commonpath([self.cvsroot_path, path])) + 1 :
                        ]
                    parent_path = os.path.dirname(path)

                    if parent_path.split("/")[-1] == "Attic":
                        continue
                    attic_path = parent_path + "/Attic"
                    if attic_path in attic_paths:
                        continue
                    attic_paths.append(attic_path)  # avoid multiple visits
                    # Try to fetch more rlog data from this Attic directory.
                    attic_rlog_file = self.cvsclient.fetch_rlog(
                        path=attic_path,
                        state="dead",
                    )
                    if attic_rlog_file:
                        attic_rlog_files.append(attic_rlog_file)
            if len(attic_rlog_files) == 0:
                self.rlog_file = main_rlog_file
            else:
                # Combine all the rlog pieces we found and re-parse.
                fp = tempfile.TemporaryFile()
                for attic_rlog_file in attic_rlog_files:
                    for line in attic_rlog_file.readlines():
                        fp.write(line)
                        attic_rlog_file.close()
                main_rlog_file.seek(0)
                for line in main_rlog_file.readlines():
                    fp.write(line)
                main_rlog_file.close()
                fp.seek(0)
                self.rlog.parse_rlog(cast(BinaryIO, fp))
                self.rlog_file = cast(BinaryIO, fp)
            cvs_changesets = sorted(self.rlog.changesets)
            self.log.debug(
                "CVS changesets found for %s: %d",
                self.cvs_module_name,
                len(cvs_changesets),
            )
            self.swh_revision_gen = self.process_cvs_changesets(
                cvs_changesets, use_rcsparse=False
            )
        else:
            raise NotFound(f"Invalid CVS origin URL '{self.origin.url}'")

    def fetch_data(self) -> bool:
        """Fetch the next CVS revision."""
        try:
            data = next(self.swh_revision_gen)
        except StopIteration:
            assert self._last_revision is not None
            self.snapshot = self.generate_and_load_snapshot(self._last_revision)
            self.log.debug(
                "SWH snapshot ID: %s", hashutil.hash_to_hex(self.snapshot.id)
            )
            self.flush()
            self.loaded_snapshot_id = self.snapshot.id
            return False
        except Exception:
            self.log.exception("Exception in fetch_data:")
            sentry_sdk.capture_exception()
            self._visit_status = "failed"
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
        self.flush()
        self._skipped_contents = []
        self._contents = []
        self._directories = []
        self._revisions = []

    def load_status(self) -> Dict[str, Any]:
        if self.snapshot is None:
            load_status = "failed"
        elif self.last_snapshot == self.snapshot:
            load_status = "uneventful"
        else:
            load_status = "eventful"
        return {
            "status": load_status,
        }

    def visit_status(self) -> str:
        return self._visit_status
