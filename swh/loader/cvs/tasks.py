# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from typing import Optional

from celery import shared_task
import iso8601

from .loader import CvsLoader


def convert_to_datetime(date: Optional[str]) -> Optional[datetime]:
    try:
        return iso8601.parse_date(date)
    except Exception:
        return None


@shared_task(name=__name__ + ".LoadCvsRepository")
def load_cvs(
    *,
    url: str,
    origin_url: Optional[str] = None,
    destination_path: Optional[str] = None,
    swh_revision: Optional[str] = None,
    visit_date: Optional[str] = None,
):
    """Import a CVS repository

    Args:
        - url: (mandatory) CVS's repository url to ingest data from
        - origin_url: Optional original url override to use as origin reference
            in the archive. If not provided, "url" is used as origin.
        - destination_path: (optional) root directory to
          locally retrieve svn's data
        - swh_revision: (optional) extra revision hex to
          start from. See swh.loader.svn.CvsLoader.process
          docstring
        - visit_date: Optional date to override the visit date
    """
    loader = CvsLoader.from_configfile(
        url=url,
        origin_url=origin_url,
        destination_path=destination_path,
        swh_revision=swh_revision,
        visit_date=convert_to_datetime(visit_date),
    )
    return loader.load()
