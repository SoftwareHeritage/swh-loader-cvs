# Copyright (C) 2016-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import subprocess
import tempfile
from typing import Any, Dict
from urllib.parse import urlparse

import pytest

from swh.loader.cvs.cvsclient import CVSClient
from swh.loader.cvs.loader import BadPathException, CvsLoader
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Snapshot, SnapshotBranch, TargetType

RUNBABY_SNAPSHOT = Snapshot(
    id=hash_to_bytes("e64667c400049f560a3856580e0d9e511ffa66c9"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("0f6db8ce49472d7829ddd6141f71c68c0d563f0e"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_not_found_no_mock(swh_storage, tmp_path):
    """Given an unknown repository, the loader visit ends up in status not_found"""
    unknown_repo_url = "unknown-repository"
    loader = CvsLoader(swh_storage, unknown_repo_url, cvsroot_path=tmp_path)

    assert loader.load() == {"status": "uneventful"}

    assert_last_visit_matches(
        swh_storage,
        unknown_repo_url,
        status="not_found",
        type="cvs",
    )


def test_loader_cvs_ssh_module_not_found(swh_storage, tmp_path, mocker):
    url = "ssh://anoncvs@anoncvs.example.org/cvsroot/foo"

    mocker.patch("swh.loader.cvs.cvsclient.socket")
    mocker.patch("swh.loader.cvs.cvsclient.subprocess")
    from swh.loader.cvs.loader import CVSClient as Client

    conn_read_line = mocker.patch.object(Client, "conn_read_line")
    conn_read_line.side_effect = [
        # server response lines when client is initialized
        b"Valid-requests ",
        b"ok\n",
        # server response line when CVS module is missing
        "E cvs rlog: cannot find module `foo' - ignored\n".encode(),
    ]

    loader = CvsLoader(swh_storage, url, cvsroot_path=tmp_path)

    assert loader.load() == {"status": "uneventful"}

    assert_last_visit_matches(
        swh_storage,
        url,
        status="not_found",
        type="cvs",
    )


def test_loader_cvs_visit(swh_storage, datadir, tmp_path):
    """Eventful visit should yield 1 snapshot"""
    archive_name = "runbaby"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=RUNBABY_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 5,
        "directory": 1,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(RUNBABY_SNAPSHOT, loader.storage)


def test_loader_cvs_2_visits_no_change(swh_storage, datadir, tmp_path):
    """Eventful visit followed by uneventful visit should yield the same snapshot"""
    archive_name = "runbaby"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=RUNBABY_SNAPSHOT.id,
    )

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )
    assert loader.load() == {"status": "uneventful"}
    visit_status2 = assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=RUNBABY_SNAPSHOT.id,
    )

    assert visit_status1.date < visit_status2.date
    assert visit_status1.snapshot == visit_status2.snapshot

    stats = get_stats(loader.storage)
    assert stats["origin_visit"] == 1 + 1  # computed twice the same snapshot
    assert stats["snapshot"] == 1


GREEK_SNAPSHOT = Snapshot(
    id=hash_to_bytes("c76f8b58a6dfbe6fccb9a85b695f914aa5c4a95a"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("e138207ddd5e1965b5ab9a522bfc2e0ecd233b67"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_with_file_additions_and_deletions(swh_storage, datadir, tmp_path):
    """Eventful conversion of history with file additions and deletions"""
    archive_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name
    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 8,
        "directory": 13,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 7,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(GREEK_SNAPSHOT, loader.storage)


def test_loader_cvs_pserver_with_file_additions_and_deletions(
    swh_storage, datadir, tmp_path
):
    """Eventful CVS pserver conversion with file additions and deletions"""
    archive_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 8,
        "directory": 13,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 7,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(GREEK_SNAPSHOT, loader.storage)


GREEK_SNAPSHOT2 = Snapshot(
    id=hash_to_bytes("e3d2e8860286000f546c01aa2a3e1630170eb3b6"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("f1ff9a3c7624b1be5e5d51f9ec0abf7dcddbf0b2"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_2_visits_with_change(swh_storage, datadir, tmp_path):
    """Eventful visit followed by eventful visit should yield two snapshots"""
    archive_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name
    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 8,
        "directory": 13,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 7,
        "skipped_content": 0,
        "snapshot": 1,
    }

    archive_name2 = "greek-repository2"
    archive_path2 = os.path.join(datadir, f"{archive_name2}.tgz")
    repo_url = prepare_repository_from_archive(archive_path2, archive_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    visit_status2 = assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT2.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 10,
        "directory": 15,
        "origin": 1,
        "origin_visit": 2,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 2,
    }

    check_snapshot(GREEK_SNAPSHOT2, loader.storage)

    assert visit_status1.date < visit_status2.date
    assert visit_status1.snapshot != visit_status2.snapshot


def test_loader_cvs_visit_pserver(swh_storage, datadir, tmp_path):
    """Eventful visit to CVS pserver should yield 1 snapshot"""
    archive_name = "runbaby"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/runbaby"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=RUNBABY_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 5,
        "directory": 1,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(RUNBABY_SNAPSHOT, loader.storage)


GREEK_SNAPSHOT3 = Snapshot(
    id=hash_to_bytes("6e9910ed072662cb482d9017cbf5e1973e6dc09f"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("d9f4837dc55a87d83730c6e277c88b67dae80272"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_visit_pserver_no_eol(swh_storage, datadir, tmp_path):
    """Visit to CVS pserver with file that lacks trailing eol"""
    archive_name = "greek-repository3"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT3.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 9,
        "directory": 15,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(GREEK_SNAPSHOT3, loader.storage)


GREEK_SNAPSHOT4 = Snapshot(
    id=hash_to_bytes("a8593e9233601b31e012d36975f817d2c993d04b"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("51bb99655225c810ee259087fcae505899725360"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_visit_expand_id_keyword(swh_storage, datadir, tmp_path):
    """Visit to CVS repository with file with an RCS Id keyword"""
    archive_name = "greek-repository4"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT4.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 12,
        "directory": 20,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 11,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(GREEK_SNAPSHOT4, loader.storage)


def test_loader_cvs_visit_pserver_expand_id_keyword(swh_storage, datadir, tmp_path):
    """Visit to CVS pserver with file with an RCS Id keyword"""
    archive_name = "greek-repository4"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT4.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 12,
        "directory": 20,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 11,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(GREEK_SNAPSHOT4, loader.storage)


GREEK_SNAPSHOT5 = Snapshot(
    id=hash_to_bytes("6484ec9bfff677731cbb6d2bd5058dabfae952ed"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("514b3bef07d56e393588ceda18cc1dfa2dc4e04a"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_with_file_deleted_and_readded(swh_storage, datadir, tmp_path):
    """Eventful conversion of history with file deletion and re-addition"""
    archive_name = "greek-repository5"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT5.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 9,
        "directory": 14,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(GREEK_SNAPSHOT5, loader.storage)


def test_loader_cvs_pserver_with_file_deleted_and_readded(
    swh_storage, datadir, tmp_path
):
    """Eventful pserver conversion with file deletion and re-addition"""
    archive_name = "greek-repository5"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT5.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 9,
        "directory": 14,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(GREEK_SNAPSHOT5, loader.storage)


DINO_SNAPSHOT = Snapshot(
    id=hash_to_bytes("6cf774cec1030ff3e9a301681303adb537855d09"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("b7d3ea1fa878d51323b5200ad2c6ee9d5b656f10"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_readded_file_in_attic(swh_storage, datadir, tmp_path):
    """Conversion of history with RCS files in the Attic"""
    # This repository has some file revisions marked "dead" in the Attic only.
    # This is different to the re-added file tests above, where the RCS file
    # was moved out of the Attic again as soon as the corresponding deleted
    # file was re-added. Failure to detect the "dead" file revisions in the
    # Attic would result in errors in our converted history.
    archive_name = "dino-readded-file"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/src"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=DINO_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 38,
        "directory": 70,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 35,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(DINO_SNAPSHOT, loader.storage)


def test_loader_cvs_pserver_readded_file_in_attic(swh_storage, datadir, tmp_path):
    """Conversion over pserver with RCS files in the Attic"""
    # This repository has some file revisions marked "dead" in the Attic only.
    # This is different to the re-added file tests above, where the RCS file
    # was moved out of the Attic again as soon as the corresponding deleted
    # file was re-added. Failure to detect the "dead" file revisions in the
    # Attic would result in errors in our converted history.
    # This has special implications for the pserver case, because the "dead"
    # revisions will not appear in in the output of 'cvs rlog' by default.
    archive_name = "dino-readded-file"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/src"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=DINO_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 38,
        "directory": 70,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 35,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(DINO_SNAPSHOT, loader.storage)


DINO_SNAPSHOT2 = Snapshot(
    id=hash_to_bytes("afdeca6b8ec8f58367b4e014e2210233f1c5bf3d"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("84e428103d42b84713c77afb9420d667062f8676"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_split_commits_by_commitid(swh_storage, datadir, tmp_path):
    """Conversion of RCS history which needs to be split by commit ID"""
    # This repository has some file revisions which use the same log message
    # and can only be told apart by commit IDs. Without commit IDs, these commits
    # would get merged into a single commit in our conversion result.
    archive_name = "dino-commitid"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/dino"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=DINO_SNAPSHOT2.id,
    )

    check_snapshot(DINO_SNAPSHOT2, loader.storage)

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 18,
        "directory": 18,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 18,
        "skipped_content": 0,
        "snapshot": 1,
    }


def test_loader_cvs_pserver_split_commits_by_commitid(swh_storage, datadir, tmp_path):
    """Conversion via pserver which needs to be split by commit ID"""
    # This repository has some file revisions which use the same log message
    # and can only be told apart by commit IDs. Without commit IDs, these commits
    # would get merged into a single commit in our conversion result.
    archive_name = "dino-commitid"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/dino"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=DINO_SNAPSHOT2.id,
    )

    check_snapshot(DINO_SNAPSHOT2, loader.storage)

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 18,
        "directory": 18,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 18,
        "skipped_content": 0,
        "snapshot": 1,
    }


GREEK_SNAPSHOT6 = Snapshot(
    id=hash_to_bytes("859ae7ca5b31fee594c98abecdd41eff17cae079"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("fa48fb4551898cd8d3305cace971b3b95639e83e"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_empty_lines_in_log_message(swh_storage, datadir, tmp_path):
    """Conversion of RCS history with empty lines in a log message"""
    archive_name = "greek-repository6"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT6.id,
    )

    check_snapshot(GREEK_SNAPSHOT6, loader.storage)

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 9,
        "directory": 14,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 1,
    }


def test_loader_cvs_pserver_empty_lines_in_log_message(swh_storage, datadir, tmp_path):
    """Conversion via pserver with empty lines in a log message"""
    archive_name = "greek-repository6"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT6.id,
    )

    check_snapshot(GREEK_SNAPSHOT6, loader.storage)

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 9,
        "directory": 14,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 1,
    }


def get_head_revision_paths_info(loader: CvsLoader) -> Dict[bytes, Dict[str, Any]]:
    assert loader.snapshot is not None
    root_dir = loader.snapshot.branches[b"HEAD"].target
    revision = loader.storage.revision_get([root_dir])[0]
    assert revision is not None

    paths = {}
    for entry in loader.storage.directory_ls(revision.directory, recursive=True):
        paths[entry["name"]] = entry
    return paths


def test_loader_cvs_with_header_keyword(swh_storage, datadir, tmp_path):
    """Eventful conversion of history with Header keyword in a file"""
    archive_name = "greek-repository7"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name
    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    repo_url = f"fake://{repo_url[7:]}"
    loader2 = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader2.load() == {"status": "eventful"}

    # We cannot verify the snapshot ID. It is unpredictable due to use of the $Header$
    # RCS keyword which contains the temporary directory where the repository is stored.

    expected_stats = {
        "content": 9,
        "directory": 14,
        "origin": 2,
        "origin_visit": 2,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 1,
    }
    stats = get_stats(loader.storage)
    assert stats == expected_stats
    stats = get_stats(loader2.storage)
    assert stats == expected_stats

    # Ensure that file 'alpha', which contains a $Header$ keyword,
    # was imported with equal content via file:// and fake:// URLs.

    paths = get_head_revision_paths_info(loader)
    paths2 = get_head_revision_paths_info(loader2)

    alpha = paths[b"alpha"]
    alpha2 = paths2[b"alpha"]
    assert alpha["sha1"] == alpha2["sha1"]


GREEK_SNAPSHOT8 = Snapshot(
    id=hash_to_bytes("5278a1f73ed0f804c68f72614a5f78ca5074ab9c"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("b389258fec8151d719e79da80b5e5355a48ec8bc"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_expand_log_keyword(swh_storage, datadir, tmp_path):
    """Conversion of RCS history with Log keyword in files"""
    archive_name = "greek-repository8"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT8.id,
    )

    check_snapshot(GREEK_SNAPSHOT8, loader.storage)

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 14,
        "directory": 20,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 11,
        "skipped_content": 0,
        "snapshot": 1,
    }


def test_loader_cvs_pserver_expand_log_keyword(swh_storage, datadir, tmp_path):
    """Conversion of RCS history with Log keyword in files"""
    archive_name = "greek-repository8"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT8.id,
    )

    check_snapshot(GREEK_SNAPSHOT8, loader.storage)

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 14,
        "directory": 20,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 11,
        "skipped_content": 0,
        "snapshot": 1,
    }


GREEK_SNAPSHOT9 = Snapshot(
    id=hash_to_bytes("3d08834666df7a589abea07ac409771ebe7e8fe4"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("9971cbb3b540dfe75f3bcce5021cb73d63b47df3"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_visit_expand_custom_keyword(swh_storage, datadir, tmp_path):
    """Visit to CVS repository with file with a custom RCS keyword"""
    archive_name = "greek-repository9"
    extracted_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, extracted_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, extracted_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=GREEK_SNAPSHOT9.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 9,
        "directory": 14,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(GREEK_SNAPSHOT9, loader.storage)


RCSBASE_SNAPSHOT = Snapshot(
    id=hash_to_bytes("2c75041ba8868df04349c1c8f4c29f992967b8aa"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("46f076387ff170dc3d4da5e43d953c1fc744c821"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_cvs_expand_log_keyword2(swh_storage, datadir, tmp_path):
    """Another conversion of RCS history with Log keyword in files"""
    archive_name = "rcsbase-log-kw-test-repo"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/src"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=RCSBASE_SNAPSHOT.id,
    )

    check_snapshot(RCSBASE_SNAPSHOT, loader.storage)

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 2,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 3,
        "skipped_content": 0,
        "snapshot": 1,
    }


def test_loader_cvs_pserver_expand_log_keyword2(swh_storage, datadir, tmp_path):
    """Another conversion of RCS history with Log keyword in files"""
    archive_name = "rcsbase-log-kw-test-repo"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/src"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=RCSBASE_SNAPSHOT.id,
    )

    check_snapshot(RCSBASE_SNAPSHOT, loader.storage)

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 2,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 3,
        "skipped_content": 0,
        "snapshot": 1,
    }


@pytest.mark.parametrize(
    "rlog_unsafe_path",
    [
        # paths that walk to parent directory:
        "unsafe_rlog_with_unsafe_relative_path.rlog",
        # absolute path outside the CVS server's root directory:
        "unsafe_rlog_wrong_arborescence.rlog",
    ],
)
def test_loader_cvs_weird_paths_in_rlog(
    swh_storage, datadir, tmp_path, mocker, rlog_unsafe_path
):
    """Handle cvs rlog output which contains unsafe paths"""
    archive_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    # Ask our cvsclient to connect via the 'cvs server' command
    repo_url = f"fake://{repo_url[7:]}"

    # And let's pretend the server returned this rlog output instead of
    # what it would actually return.
    rlog_file = tempfile.NamedTemporaryFile(
        dir=tmp_path, mode="w+", delete=False, prefix="weird-path-rlog-"
    )
    rlog_file_path = rlog_file.name

    rlog_weird_paths = open(os.path.join(datadir, rlog_unsafe_path))
    for line in rlog_weird_paths:
        rlog_file.write(line.replace("{cvsroot_path}", os.path.dirname(repo_url[7:])))
    rlog_file.close()
    rlog_file_override = open(rlog_file_path, "rb")  # re-open as bytes instead of str
    mock_read = mocker.patch("swh.loader.cvs.cvsclient.CVSClient.fetch_rlog")
    mock_read.return_value = rlog_file_override

    def side_effect(self, path="", state=""):
        return None

    mock_read.side_effect = side_effect(side_effect)

    try:
        loader = CvsLoader(
            swh_storage,
            repo_url,
            cvsroot_path=os.path.join(tmp_path, archive_name),
        )
    except BadPathException:
        pass

    assert loader.load()["status"] == "failed"

    assert_last_visit_matches(
        swh_storage,
        repo_url,
        status="failed",
        type="cvs",
    )

    assert mock_read.called

    rlog_file_override.close()
    os.unlink(rlog_file_path)


def test_loader_rsync_retry(swh_storage, mocker, tmp_path):
    module_name = "module"
    host = "example.org"
    path = f"/cvsroot/{module_name}"
    repo_url = f"rsync://{host}{path}/"

    rsync_first_call = ["rsync", repo_url]
    rsync_second_call = [
        "rsync",
        "-az",
        f"{repo_url}CVSROOT/",
        os.path.join(tmp_path, "CVSROOT/"),
    ]
    rsync_third_call = [
        "rsync",
        "-az",
        f"{repo_url}{module_name}/",
        os.path.join(tmp_path, f"{module_name}/"),
    ]

    mock_subprocess = mocker.patch("swh.loader.cvs.loader.subprocess")
    mock_subprocess.run.side_effect = [
        subprocess.CompletedProcess(args=rsync_first_call, returncode=23),
        subprocess.CompletedProcess(
            args=rsync_first_call,
            returncode=0,
            stdout=f"""
            drwxr-xr-x             21 2012/11/04 06:58:58 .
            drwxr-xr-x             39 2021/01/22 10:21:05 CVSROOT
            drwxr-xr-x             15 2020/12/28 00:50:21 {module_name}""",
        ),
        subprocess.CompletedProcess(
            args=rsync_second_call,
            returncode=23,
        ),
        subprocess.CompletedProcess(
            args=rsync_second_call,
            returncode=23,
        ),
        subprocess.CompletedProcess(args=rsync_second_call, returncode=0),
        subprocess.CompletedProcess(
            args=rsync_third_call,
            returncode=23,
        ),
        subprocess.CompletedProcess(
            args=rsync_third_call,
            returncode=23,
        ),
        subprocess.CompletedProcess(args=rsync_third_call, returncode=0),
    ]

    loader = CvsLoader(swh_storage, repo_url)
    loader.cvs_module_name = module_name
    loader.cvsroot_path = tmp_path
    loader.fetch_cvs_repo_with_rsync(host, path)


@pytest.mark.parametrize(
    "pserver_url",
    [
        "pserver://anonymous:anonymous@cvs.example.org/cvsroot/project/module",
        "pserver://anonymous@cvs.example.org/cvsroot/project/module",
    ],
)
def test_cvs_client_connect_pserver(mocker, pserver_url):
    from swh.loader.cvs.cvsclient import socket

    conn = mocker.MagicMock()
    conn.recv.side_effect = [b"I LOVE YOU\n", b"Valid-requests \n", b"ok\n"]
    mocker.patch.object(socket, "create_connection").return_value = conn
    parsed_url = urlparse(pserver_url)

    # check cvs client can be instantiated without errors
    CVSClient(parsed_url)


@pytest.mark.parametrize("protocol", ["rsync", "pserver"])
def test_loader_cvs_with_non_utf8_directory_paths(
    swh_storage, datadir, tmp_path, protocol
):
    archive_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/greek-tree"  # CVS module name

    protocol_prefix = "file://"
    if protocol == "pserver":
        protocol_prefix = "fake://"
        repo_url = repo_url.replace("file://", protocol_prefix)

    for root, _, files in os.walk(repo_url.replace(protocol_prefix, "")):
        for file in files:
            # clone existing file in repository but makes it path non UTF-8 encoded
            filepath = os.path.join(root, file)
            with open(filepath, "rb") as f:
                filecontent = f.read()
            filepath = root.encode() + ("é" + file).encode("iso-8859-1")
            with open(filepath, "wb") as f:
                f.write(filecontent)

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}


CPMIXIN_SNAPSHOT = Snapshot(
    id=hash_to_bytes("105b49290a48cc780f5519588ae822e2dd942930"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("658f18d145376f0b71716649602752b509cfdbd4"),
            target_type=TargetType.REVISION,
        )
    },
)


@pytest.mark.parametrize("protocol", ["rsync", "pserver"])
def test_loader_cvs_with_rev_numbers_greater_than_one(
    swh_storage, datadir, tmp_path, protocol
):
    archive_name = "cpmixin"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/cpmixin"  # CVS module name

    protocol_prefix = "file://"
    if protocol == "pserver":
        protocol_prefix = "fake://"
        repo_url = repo_url.replace("file://", protocol_prefix)

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=CPMIXIN_SNAPSHOT.id,
    )

    check_snapshot(CPMIXIN_SNAPSHOT, loader.storage)


def test_loader_cvs_with_missing_cvs_config_file(swh_storage, datadir, tmp_path):
    archive_name = "greek-repository"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    config_path = os.path.join(repo_url.replace("file://", ""), "CVSROOT/config")

    assert os.path.exists(config_path)
    os.remove(config_path)
    repo_url += "/greek-tree"  # CVS module name
    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "eventful"}


def test_loader_cvs_rsync_not_found(swh_storage, mocker):
    origin_url = "rsync://example.org/cvsroot/module"
    loader = CvsLoader(swh_storage, origin_url)
    mocker.patch.object(loader, "execute_rsync").side_effect = (
        subprocess.CalledProcessError(
            returncode=23,
            cmd=["rsync", origin_url],
            stderr=(
                'rsync: change_dir "/module" (in cvsroot) failed: '
                "No such file or directory (2)"
            ),
        )
    )
    assert loader.load() == {"status": "uneventful"}
    visit_status = (
        swh_storage.origin_visit_get_with_statuses(origin_url).results[-1].statuses[-1]
    )
    assert visit_status.status == "not_found"


def test_loader_cvs_empty_repository(swh_storage, datadir, tmp_path):
    archive_name = "alizagameapi"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_url += "/config"  # CVS module name

    loader = CvsLoader(
        swh_storage, repo_url, cvsroot_path=os.path.join(tmp_path, archive_name)
    )

    assert loader.load() == {"status": "uneventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="cvs",
        snapshot=Snapshot(branches={}).id,
    )


def test_loader_cvs_max_content_size(swh_storage, datadir, tmp_path):
    """Contents whose size is greater than 1 byte should be skipped."""
    archive_name = "runbaby"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = CvsLoader(
        swh_storage,
        repo_url,
        cvsroot_path=os.path.join(tmp_path, archive_name),
        max_content_size=1,
    )

    assert loader.load() == {"status": "eventful"}

    assert get_stats(loader.storage) == {
        "content": 0,
        "directory": 1,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 5,
        "snapshot": 1,
    }
