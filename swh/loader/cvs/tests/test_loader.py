# Copyright (C) 2016-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import pytest
from swh.loader.cvs.loader import CvsLoader
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Snapshot, SnapshotBranch, TargetType

def test_loader_cvs_not_found_no_mock(swh_storage, tmp_path):
    """Given an unknown repository, the loader visit ends up in status not_found"""
    unknown_repo_url = "unknown-repository"
    loader = CvsLoader(swh_storage, unknown_repo_url, destination_path=tmp_path)

    assert loader.load() == {"status": "uneventful"}

    assert_last_visit_matches(
        swh_storage, unknown_repo_url, status="not_found", type="cvs",
    )


def test_loader_svn_cvs_visit(swh_storage, datadir, tmp_path):
    """Eventful visit should yield 1 snapshot"""
    archive_name = "runbaby"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = CvsLoader(swh_storage, repo_url, destination_path=tmp_path)

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 19,
        "directory": 17,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 6,
        "skipped_content": 0,
        "snapshot": 1,
    }

    #check_snapshot(GOURMET_SNAPSHOT, loader.storage)
