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
        ("23112021", None,),  # failure to parse
    ],
)
def test_convert_to_datetime(date, expected_result):
    assert convert_to_datetime(date) == expected_result
