# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import uuid

import pytest

from swh.loader.tests import assert_module_tasks_are_scheduler_ready
from swh.scheduler.model import ListedOrigin, Lister

NAMESPACE = "swh.loader.cvs"


def test_tasks_loader_visit_type_match_task_name():
    import swh.loader.cvs

    assert_module_tasks_are_scheduler_ready([swh.loader.cvs])


@pytest.fixture
def cvs_lister():
    return Lister(name="cvs-lister", instance_name="example", id=uuid.uuid4())


@pytest.fixture
def cvs_listed_origin(cvs_lister):
    return ListedOrigin(
        lister_id=cvs_lister.id,
        url="rsync://cvs.example.org/cvsroot/module",
        visit_type="cvs",
    )


@pytest.mark.parametrize("extra_loader_arguments", [{}, {"visit_date": "now"}])
def test_cvs_loader_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    cvs_lister,
    cvs_listed_origin,
    extra_loader_arguments,
):
    cvs_listed_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.loader.CvsLoader",
        task_function_name=f"{NAMESPACE}.tasks.LoadCvsRepository",
        lister=cvs_lister,
        listed_origin=cvs_listed_origin,
    )
