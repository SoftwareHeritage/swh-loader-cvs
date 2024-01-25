# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
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


def test_cvs_client_checkout_log_kw_expansion_skipped(mocker, tmp_path):
    url = "ssh://anoncvs@anoncvs.example.org/cvsroot/src"

    file_path = b"src/foo"
    file_content = b"File with log keyword expansion in it: $Log$"
    file_rev = b"1.1.1.1"

    mocker.patch("swh.loader.cvs.cvsclient.socket")
    mocker.patch("swh.loader.cvs.cvsclient.subprocess")
    conn_read_line = mocker.patch.object(CVSClient, "conn_read_line")
    conn_read_line.side_effect = [
        # server response lines when client is initialized
        b"Valid-requests ",
        b"ok\n",
        # server response when attempting to checkout file
        b"E cvs checkout: Skipping `$Log$' keyword due to excessive comment leader.\n",
        b"MT +updated\n",
        b"MT text U \n",
        b"MT fname " + file_path + b"\n",
        b"MT newline\n",
        b"MT -updated\n",
        b"Created " + file_path + b"\n",
        file_path + b"\n",
        b"/foo/" + file_rev + b"///" + file_rev + b"\n",
        b"u=rw,g=r,o=r\n",
        str(len(file_content)).encode() + b"\n",
        file_content,
        b"ok\n",
    ]

    client = CVSClient(urlparse(url))

    dest_path = os.path.join(str(tmp_path).encode(), os.path.basename(file_path))

    client.checkout(
        file_path, file_rev.decode(), dest_path=dest_path, expand_keywords=True
    )

    with open(dest_path, "rb") as checkout_file:
        assert checkout_file.read() == file_content
