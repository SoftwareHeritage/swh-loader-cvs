# Copyright (C) 2016-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Any, Dict

from swh.loader.cvs.loader import CvsLoader
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
        swh_storage, unknown_repo_url, status="not_found", type="cvs",
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
    """Eventful visit followed by uneventful visit should yield the same snapshot

    """
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
        loader.storage, repo_url, status="full", type="cvs", snapshot=GREEK_SNAPSHOT.id,
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
        loader.storage, repo_url, status="full", type="cvs", snapshot=GREEK_SNAPSHOT.id,
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
        loader.storage, repo_url, status="full", type="cvs", snapshot=GREEK_SNAPSHOT.id,
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
        loader.storage, repo_url, status="full", type="cvs", snapshot=DINO_SNAPSHOT.id,
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
        loader.storage, repo_url, status="full", type="cvs", snapshot=DINO_SNAPSHOT.id,
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
        loader.storage, repo_url, status="full", type="cvs", snapshot=DINO_SNAPSHOT2.id,
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
        loader.storage, repo_url, status="full", type="cvs", snapshot=DINO_SNAPSHOT2.id,
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

    # We cannot verify the snapshot ID. It is unpredicable due to use of the $Header$
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
