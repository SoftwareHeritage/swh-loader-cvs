# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

from urllib.parse import urlparse

from swh.loader.cvs.cvsclient import CVSClient


def test_cvs_client_rlog_could_not_read_rcs_file(mocker):

    url = "ssh://anoncvs@anoncvs.example.org/cvsroot/src"
    file = "src/README.txt"

    mocker.patch("swh.loader.cvs.cvsclient.socket")
    mocker.patch("swh.loader.cvs.cvsclient.subprocess")
    conn_read_line = mocker.patch.object(CVSClient, "conn_read_line")
    conn_read_line.side_effect = [
        # server response lines when client is initialized
        b"Valid-requests ",
        b"ok\n",
        # server response lines when attempting to fetch rlog of a file
        # but RCS file is missing
        f"E cvs rlog: could not read RCS file for {file}\n".encode(),
        b"ok\n",
    ]

    client = CVSClient(urlparse(url))

    assert client.fetch_rlog(file.encode()) is None
