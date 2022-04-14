# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone

import pytest

from swh.loader.cvs.tasks import convert_to_datetime


@pytest.mark.parametrize(
    "date,expected_result",
    [
        (None, None),
        (
            "2021-11-23 09:41:02.434195+00:00",
            datetime(2021, 11, 23, 9, 41, 2, 434195, tzinfo=timezone.utc),
        ),
        (
            "23112021",
            None,
        ),  # failure to parse
    ],
)
def test_convert_to_datetime(date, expected_result):
    assert convert_to_datetime(date) == expected_result


def test_cvs_loader(
    mocker, swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_config
):
    mock_loader = mocker.patch("swh.loader.cvs.loader.CvsLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.cvs.tasks.LoadCvsRepository",
        kwargs=dict(url="some-technical-url", origin_url="origin-url"),
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
    assert mock_loader.called
