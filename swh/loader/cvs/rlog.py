# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

""" RCS/CVS rlog parser, derived from viewvc and cvs2gitdump.py """

# Copyright (C) 1999-2021 The ViewCVS Group. All Rights Reserved.
#
# By using ViewVC, you agree to the terms and conditions set forth
# below:
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#   * Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following
#     disclaimer.
#
#   * Redistributions in binary form must reproduce the above
#     copyright notice, this list of conditions and the following
#     disclaimer in the documentation and/or other materials provided
#     with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
# IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Copyright (c) 2012 YASUOKA Masahiko <yasuoka@yasuoka.net>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import calendar
import re
import time

from swh.loader.cvs.cvs2gitdump.cvs2gitdump import ChangeSetKey, file_path

# TODO: actual path encoding should be specified as a parameter
path_encodings = ["ascii", "utf-8"]


class RlogConv:
    def __init__(self, cvsroot_path, fuzzsec):
        self.cvsroot_path = cvsroot_path
        self.fuzzsec = fuzzsec
        self.changesets = dict()
        self.tags = dict()
        self.offsets = dict()

    def _process_rlog_entry(self, path, taginfo, revisions, logmsgs):
        """ Convert an rlog entry into an item in self.changesets """
        rtags = dict()
        branches = {"1": "HEAD", "1.1.1": "VENDOR"}
        for k, v in list(taginfo.items()):
            r = v.split(".")
            if len(r) == 3:
                branches[v] = "VENDOR"
            elif len(r) >= 3 and r[-2] == "0":
                branches[".".join(r[:-2] + r[-1:])] = k
            if len(r) == 2 and branches[r[0]] == "HEAD":
                if v not in rtags:
                    rtags[v] = list()
                rtags[v].append(k)

        revs = revisions.items()
        # sort by revision descending to priorize 1.1.1.1 than 1.1
        revs = sorted(revs, key=lambda a: a[1][0], reverse=True)
        # sort by time
        revs = sorted(revs, key=lambda a: a[1][1])
        novendor = False
        have_initial_revision = False
        last_vendor_status = None
        for k, v in revs:
            r = k.split(".")
            if (
                len(r) == 4
                and r[0] == "1"
                and r[1] == "1"
                and r[2] == "1"
                and r[3] == "1"
            ):
                if have_initial_revision:
                    continue
                if v[3] == "dead":
                    continue
                last_vendor_status = v[3]
                have_initial_revision = True
            elif len(r) == 4 and r[0] == "1" and r[1] == "1" and r[2] == "1":
                if novendor:
                    continue
                last_vendor_status = v[3]
            elif len(r) == 2:
                if r[0] == "1" and r[1] == "1":
                    if have_initial_revision:
                        continue
                    if v[3] == "dead":
                        continue
                    have_initial_revision = True
                elif r[0] == "1" and r[1] != "1":
                    novendor = True
                if last_vendor_status == "dead" and v[3] == "dead":
                    last_vendor_status = None
                    continue
                last_vendor_status = None
            else:
                # trunk only
                continue

            b = ".".join(r[:-1])
            # decode author name in a potentially lossy way;
            # it is only used for internal hashing in this case
            author = v[2].decode("utf-8", "ignore")
            a = ChangeSetKey(branches[b], author, v[1], logmsgs[k], v[6], self.fuzzsec)

            a.put_file(path, k, v[3], 0)
            while a in self.changesets:
                c = self.changesets[a]
                del self.changesets[a]
                c.merge(a)
                a = c
            self.changesets[a] = a
            if k in rtags:
                for t in rtags[k]:
                    if t not in self.tags or self.tags[t].max_time < a.max_time:
                        self.tags[t] = a

    def parse_rlog(self, fp):
        eof = None
        while eof != _EOF_LOG and eof != _EOF_ERROR:
            filename, branch, taginfo, lockinfo, errmsg, eof = _parse_log_header(fp)
            revisions = {}
            logmsgs = {}
            if filename:
                for i, e in enumerate(path_encodings):
                    try:
                        how = "ignore" if i == len(path_encodings) - 1 else "strict"
                        fname = filename.decode(e, how)
                        break
                    except UnicodeError:
                        pass
            while not eof:
                off = fp.tell()
                rev, logmsg, eof = _parse_log_entry(fp)
                if rev:
                    revisions[rev[0]] = rev
                    logmsgs[rev[0]] = logmsg
            if eof != _EOF_LOG and eof != _EOF_ERROR:
                path = file_path(self.cvsroot_path, fname)
                if not path in self.offsets.keys():
                    self.offsets[path] = dict()
                if rev:
                    self.offsets[path][rev[0]] = off
                self._process_rlog_entry(path, taginfo, revisions, logmsgs)

    def getlog(self, fp, path, rev):
        off = self.offsets[path][rev]
        fp.seek(off)
        rev, logmsg, eof = _parse_log_entry(fp)
        return logmsg


# if your rlog doesn't use 77 '=' characters, then this must change
LOG_END_MARKER = b"=" * 77 + b"\n"
ENTRY_END_MARKER = b"-" * 28 + b"\n"

_EOF_FILE = b"end of file entries"  # no more entries for this RCS file
_EOF_LOG = b"end of log"  # hit the true EOF on the pipe
_EOF_ERROR = b"error message found"  # rlog issued an error

# rlog error messages look like
#
#   rlog: filename/goes/here,v: error message
#   rlog: filename/goes/here,v:123: error message
#
# so we should be able to match them with a regex like
#
#   ^rlog\: (.*)(?:\:\d+)?\: (.*)$
#
# But for some reason the windows version of rlog omits the "rlog: " prefix
# for the first error message when the standard error stream has been
# redirected to a file or pipe. (the prefix is present in subsequent errors
# and when rlog is run from the console). So the expression below is more
# complicated
_re_log_error = re.compile(b"^(?:rlog\: )*(.*,v)(?:\:\d+)?\: (.*)$")

# CVSNT error messages look like:
# cvs rcsfile: `C:/path/to/file,v' does not appear to be a valid rcs file
# cvs [rcsfile aborted]: C:/path/to/file,v: No such file or directory
# cvs [rcsfile aborted]: cannot open C:/path/to/file,v: Permission denied
_re_cvsnt_error = re.compile(
    b"^(?:cvs rcsfile\: |cvs \[rcsfile aborted\]: )"
    b"(?:\`(.*,v)' |"
    b"cannot open (.*,v)\: |(.*,v)\: |)"
    b"(.*)$"
)


def _parse_log_header(fp):
    """Parse and RCS/CVS log header.

  fp is a file (pipe) opened for reading the log information.

  On entry, fp should point to the start of a log entry.
  On exit, fp will have consumed the separator line between the header and
  the first revision log.

  If there is no revision information (e.g. the "-h" switch was passed to
  rlog), then fp will consumed the file separator line on exit.

  Returns: filename, default branch, tag dictionary, lock dictionary,
  rlog error message, and eof flag
  """

    filename = branch = msg = b""
    taginfo = {}  # tag name => number
    lockinfo = {}  # revision => locker
    state = 0  # 0 = base, 1 = parsing symbols, 2 = parsing locks
    eof = None

    while 1:
        line = fp.readline()
        if not line:
            # the true end-of-file
            eof = _EOF_LOG
            break

        if state == 1:
            if line[0] == b"\t":
                [tag, rev] = [x.strip() for x in line.split(b":")]
                taginfo[tag] = rev
            else:
                # oops. this line isn't tag info. stop parsing tags.
                state = 0

        if state == 2:
            if line[0] == b"\t":
                [locker, rev] = [x.strip() for x in line.split(b":")]
                lockinfo[rev] = locker
            else:
                # oops. this line isn't lock info. stop parsing tags.
                state = 0

        if state == 0:
            if line[:9] == b"RCS file:":
                filename = line[10:-1]
            elif line[:5] == b"head:":
                # head = line[6:-1]
                pass
            elif line[:7] == b"branch:":
                branch = line[8:-1]
            elif line[:6] == b"locks:":
                # start parsing the lock information
                state = 2
            elif line[:14] == b"symbolic names":
                # start parsing the tag information
                state = 1
            elif line == ENTRY_END_MARKER:
                # end of the headers
                break
            elif line == LOG_END_MARKER:
                # end of this file's log information
                eof = _EOF_FILE
                break
            else:
                error = _re_cvsnt_error.match(line)
                if error:
                    p1, p2, p3, msg = error.groups()
                    filename = p1 or p2 or p3
                    if not filename:
                        raise vclib.Error(
                            "Could not get filename from CVSNT error:\n%s" % line
                        )
                    eof = _EOF_ERROR
                    break

                error = _re_log_error.match(line)
                if error:
                    filename, msg = error.groups()
                    if msg[:30] == b"warning: Unknown phrases like ":
                        # don't worry about this warning. it can happen with some RCS
                        # files that have unknown fields in them (e.g. "permissions 644;"
                        continue
                    eof = _EOF_ERROR
                    break

    return filename, branch, taginfo, lockinfo, msg, eof


_re_log_info = re.compile(
    b"^date:\s+([^;]+);"
    b"\s+author:\s+([^;]+);"
    b"\s+state:\s+([^;]+);"
    b"(\s+lines:\s+([0-9\s+-]+);?)?"
    b"(\s+commitid:\s+([a-zA-Z0-9]+))?\n$"
)

# TODO: _re_rev should be updated to extract the "locked" flag
_re_rev = re.compile(b"^revision\s+([0-9.]+).*")


def cvs_strptime(timestr):
    try:
        return time.strptime(timestr, "%Y/%m/%d %H:%M:%S")[:-1] + (0,)
    except ValueError:
        return time.strptime(timestr, "%Y-%m-%d %H:%M:%S %z")[:-1] + (0,)


def _parse_log_entry(fp):
    """Parse a single log entry.

  On entry, fp should point to the first line of the entry (the "revision"
  line).
  On exit, fp will have consumed the log separator line (dashes) or the
  end-of-file marker (equals).

  Returns: Revision data tuple, and eof flag (see _EOF_*)
  """
    rev = None
    line = fp.readline()
    if not line:
        return None, None, _EOF_LOG
    if line == LOG_END_MARKER:
        # Needed because some versions of RCS precede LOG_END_MARKER
        # with ENTRY_END_MARKER
        return None, None, _EOF_FILE
    if line[:8] == b"revision":
        match = _re_rev.match(line)
        if not match:
            return None, None, _EOF_LOG
        rev = match.group(1)

        line = fp.readline()
        if not line:
            return None, None, _EOF_LOG
        match = _re_log_info.match(line)

    eof = None
    log = b""
    while 1:
        line = fp.readline()
        if not line:
            # true end-of-file
            eof = _EOF_LOG
            break
        if line[:9] == b"branches:":
            continue
        if line == ENTRY_END_MARKER:
            break
        if line == LOG_END_MARKER:
            # end of this file's log information
            eof = _EOF_FILE
            break

        log = log + line

    if not rev or not match:
        # there was a parsing error
        return None, None, eof

    # parse out a time tuple for the local time
    tm = cvs_strptime(match.group(1).decode("UTF-8"))

    # rlog seems to assume that two-digit years are 1900-based (so, "04"
    # comes out as "1904", not "2004").
    EPOCH = 1970
    if tm[0] < EPOCH:
        tm = list(tm)
        if (tm[0] - 1900) < 70:
            tm[0] = tm[0] + 100
        if tm[0] < EPOCH:
            raise ValueError("invalid year")
    date = calendar.timegm(tm)

    # return a revision tuple compatible with 'rcsparse', the log message, and the EOF marker
    return (
        (
            rev.decode("ascii"),  # revision number string
            date,
            match.group(2),  # author (encoding is arbitrary; don't attempt to decode)
            match.group(3).decode(
                "ascii"
            ),  # state, usually "Exp" or "dead"; non-ASCII data here would be weird
            None,  # TODO: branches of this rev
            None,  # TODO: revnumstr of previous rev
            None,  # TODO: commitid
        ),
        log,
        eof,
    )
