# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import uuid

import pytest

from swh.scheduler.model import ListedOrigin, Lister
from swh.scheduler.utils import create_origin_task_dict


@pytest.fixture(autouse=True)
def celery_worker_and_swh_config(swh_scheduler_celery_worker, swh_config):
    pass


@pytest.fixture
def cvs_lister():
    return Lister(name="cvs-lister", instance_name="example", id=uuid.uuid4())


@pytest.fixture
def cvs_listed_origin(cvs_lister):
    return ListedOrigin(
        lister_id=cvs_lister.id, url="https://cvs.example.org/repo", visit_type="cvs"
    )


def test_cvs_loader(
    mocker,
    swh_scheduler_celery_app,
):
    mock_loader = mocker.patch("swh.loader.cvs.loader.CvsLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.cvs.tasks.LoadCvsRepository",
        kwargs=dict(
            url="some-technical-url", origin_url="origin-url", visit_date="now"
        ),
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
    assert mock_loader.called


def test_cvs_loader_for_listed_origin(
    mocker, swh_scheduler_celery_app, cvs_lister, cvs_listed_origin
):
    mock_loader = mocker.patch("swh.loader.cvs.loader.CvsLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    task_dict = create_origin_task_dict(cvs_listed_origin, cvs_lister)

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.cvs.tasks.LoadCvsRepository",
        kwargs=task_dict["arguments"]["kwargs"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
    assert mock_loader.called
