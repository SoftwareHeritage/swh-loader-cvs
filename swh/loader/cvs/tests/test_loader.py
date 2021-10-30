# Copyright (C) 2016-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

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
    id=hash_to_bytes("1cff69ab9bd70822d5e3006092f943ccaafdcf57"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("ef511d258fa55035c2bc2a5b05cad233cee1d328"),
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
        "directory": 2,
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
    id=hash_to_bytes("5e74af67d69dfd7aea0eb118154d062f71f50120"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("e18b92f14cd5b3efb3fcb4ea46cfaf97f25f301b"),
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
        "directory": 20,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 7,
        "skipped_content": 0,
        "snapshot": 7,
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
        "directory": 20,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 7,
        "skipped_content": 0,
        "snapshot": 7,
    }

    check_snapshot(GREEK_SNAPSHOT, loader.storage)


GREEK_SNAPSHOT2 = Snapshot(
    id=hash_to_bytes("048885ae2145ffe81588aea95dcf75c536ecdf26"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("55eb1438c03588607ce4b8db8f45e8e23075951b"),
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
        "directory": 20,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 7,
        "skipped_content": 0,
        "snapshot": 7,
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
        "directory": 23,
        "origin": 1,
        "origin_visit": 2,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 8,
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
    repo_url = "fake://" + repo_url[7:]

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
        "directory": 2,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(RUNBABY_SNAPSHOT, loader.storage)


GREEK_SNAPSHOT3 = Snapshot(
    id=hash_to_bytes("cd801546b0137c82f01b9b67848ba8261d64ebbb"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("14980990790ce1921db953c4c9ae03dd8861e8d6"),
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
    repo_url = "fake://" + repo_url[7:]

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
        "directory": 23,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 8,
    }

    check_snapshot(GREEK_SNAPSHOT3, loader.storage)


GREEK_SNAPSHOT4 = Snapshot(
    id=hash_to_bytes("11673e2766654bd5fafb5119b418794230d48d6b"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("fe4a926d49d2af76e0025a8ba0b4ed159aec6829"),
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
        "content": 9,
        "directory": 22,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 8,
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
        "content": 9,
        "directory": 22,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 8,
    }

    check_snapshot(GREEK_SNAPSHOT4, loader.storage)


GREEK_SNAPSHOT5 = Snapshot(
    id=hash_to_bytes("ee6faeaf50aa513c53c8ba29194116a5ef88add6"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("4320f152cc61ed660d25fdeebc787b3099e55a96"),
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
        "directory": 22,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 8,
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
        "directory": 22,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 8,
        "skipped_content": 0,
        "snapshot": 8,
    }

    check_snapshot(GREEK_SNAPSHOT5, loader.storage)


DINO_SNAPSHOT = Snapshot(
    id=hash_to_bytes("417021c16e17c5e0038cf0e73dbf48a6142c8304"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("df61a776c401a178cc796545849fc87bdadb2001"),
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
        "directory": 105,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 35,
        "skipped_content": 0,
        "snapshot": 35,
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
        "directory": 105,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 35,
        "skipped_content": 0,
        "snapshot": 35,
    }

    check_snapshot(DINO_SNAPSHOT, loader.storage)
