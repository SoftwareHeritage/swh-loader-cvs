#!/usr/local/bin/python

#
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

# Usage
#
#   First import:
#   % git init --bare /git/openbsd.git
#   % python cvs2gitdump.py -k OpenBSD -e openbsd.org /cvs/openbsd/src \
#       > openbsd.dump
#   % git --git-dir /git/openbsd.git fast-import < openbsd.dump
#
#   Periodic import:
#   % sudo cvsync
#   % python cvs2gitdump.py -k OpenBSD -e openbsd.org /cvs/openbsd/src \
#       /git/openbsd.git > openbsd2.dump
#   % git --git-dir /git/openbsd.git fast-import < openbsd2.dump
#

import copy
import getopt
import os
import re
import subprocess
import sys
import time
from typing import Dict, List, Optional, Tuple, TypeVar

import swh.loader.cvs.rcsparse as rcsparse

CHANGESET_FUZZ_SEC = 300


def usage():
    print('usage: cvs2gitdump [-ah] [-z fuzz] [-e email_domain] '
          '[-E log_encodings]\n'
          '\t[-k rcs_keywords] [-b branch] [-m module] [-l last_revision]\n'
          '\tcvsroot [git_dir]', file=sys.stderr)


def main() -> None:
    email_domain = None
    do_incremental = False
    git_tip = None
    git_branch = 'master'
    dump_all = False
    log_encoding = 'utf-8,iso-8859-1'
    rcs = RcsKeywords()
    modules = []
    last_revision = None
    fuzzsec = CHANGESET_FUZZ_SEC

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'ab:hm:z:e:E:k:t:l:')
        for opt, v in opts:
            if opt == '-z':
                fuzzsec = int(v)
            elif opt == '-e':
                email_domain = v
            elif opt == '-a':
                dump_all = True
            elif opt == '-b':
                git_branch = v
            elif opt == '-E':
                log_encoding = v
            elif opt == '-k':
                rcs.add_id_keyword(v)
            elif opt == '-m':
                if v == '.git':
                    print('Cannot handle the path named \'.git\'',
                          file=sys.stderr)
                    sys.exit(1)
                modules.append(v)
            elif opt == '-l':
                last_revision = v
            elif opt == '-h':
                usage()
                sys.exit(1)
    except getopt.GetoptError as msg:
        print(msg, file=sys.stderr)
        usage()
        sys.exit(1)

    if len(args) == 0 or len(args) > 2:
        usage()
        sys.exit(1)

    log_encodings = log_encoding.split(',')

    cvsroot = args[0]
    while cvsroot[-1] == '/':
        cvsroot = cvsroot[:-1]

    if len(args) == 2:
        do_incremental = True
        git = subprocess.Popen(
            ['git', '--git-dir=' + args[1], '-c',
             'i18n.logOutputEncoding=UTF-8', 'log', '--max-count', '1',
             '--date=raw', '--format=%ae%n%ad%n%H', git_branch],
            encoding='utf-8', stdout=subprocess.PIPE)
        assert git.stdout is not None
        outs = git.stdout.readlines()
        git.wait()
        if git.returncode != 0:
            print("Couldn't exec git", file=sys.stderr)
            sys.exit(git.returncode)
        git_tip = outs[2].strip()

        if last_revision is not None:
            git = subprocess.Popen(
                ['git', '--git-dir=' + args[1], '-c',
                 'i18n.logOutputEncoding=UTF-8', 'log', '--max-count', '1',
                 '--date=raw', '--format=%ae%n%ad%n%H', last_revision],
                encoding='utf-8', stdout=subprocess.PIPE)
            assert git.stdout is not None
            outs = git.stdout.readlines()
            git.wait()
            if git.returncode != 0:
                print("Coundn't exec git", file=sys.stderr)
                sys.exit(git.returncode)
        last_author = outs[0].strip()
        last_ctime = float(outs[1].split()[0])

        # strip off the domain part from the last author since cvs doesn't have
        # the domain part.
        if do_incremental and email_domain is not None and \
                last_author.lower().endswith(('@' + email_domain).lower()):
            last_author = last_author[:-1 * (1 + len(email_domain))]

    cvs = CvsConv(cvsroot, rcs, not do_incremental, fuzzsec)
    print('** walk cvs tree', file=sys.stderr)
    if len(modules) == 0:
        cvs.walk()
    else:
        for module in modules:
            cvs.walk(module)

    changesets = sorted(cvs.changesets)
    nchangesets = len(changesets)
    print('** cvs has %d changeset' % (nchangesets), file=sys.stderr)

    if nchangesets <= 0:
        sys.exit(0)

    if not dump_all:
        # don't use last 10 minutes for safety
        max_time_max = changesets[-1].max_time - 600
    else:
        max_time_max = changesets[-1].max_time

    found_last_revision = False
    markseq = cvs.markseq
    extags = set()
    for k in changesets:
        if do_incremental and not found_last_revision:
            if k.min_time == last_ctime and k.author == last_author:
                found_last_revision = True
            for tag in k.tags:
                extags.add(tag)
            continue
        if k.max_time > max_time_max:
            break

        marks = {}

        for f in k.revs:
            if not do_incremental:
                marks[f.markseq] = f
            else:
                markseq = markseq + 1
                git_dump_file(f.path, f.rev, rcs, markseq)
                marks[markseq] = f
        log = rcsparse.rcsfile(k.revs[0].path).getlog(k.revs[0].rev)
        for i, e in enumerate(log_encodings):
            try:
                how = 'ignore' if i == len(log_encodings) - 1 else 'strict'
                log_str = log.decode(e, how)
                break
            except UnicodeError:
                pass
        log = log_str.encode('utf-8', 'ignore')

        output('commit refs/heads/' + git_branch)
        markseq = markseq + 1
        output('mark :%d' % (markseq))
        email = k.author if email_domain is None \
            else k.author + '@' + email_domain
        output('author %s <%s> %d +0000' % (k.author, email, k.min_time))
        output('committer %s <%s> %d +0000' % (k.author, email, k.min_time))

        output('data', len(log))
        output(log, end='')
        if do_incremental and git_tip is not None:
            output('from', git_tip)
            git_tip = None

        for m in marks:
            f = marks[m]
            mode = 0o100755 if os.access(f.path, os.X_OK) else 0o100644
            fn = file_path(cvs.cvsroot, f.path)
            if f.state == 'dead':
                output('D', fn)
            else:
                output('M %o :%d %s' % (mode, m, fn))
        output('')
        for tag in k.tags:
            if tag in extags:
                continue
            output('reset refs/tags/%s' % (tag))
            output('from :%d' % (markseq))
            output('')

    if do_incremental and not found_last_revision:
        raise Exception('could not find the last revision')

    print('** dumped', file=sys.stderr)


#
# Encode by UTF-8 always for string objects since encoding for git-fast-import
# is UTF-8.  Also write without conversion for a bytes object (file bodies
# might be various encodings)
#
def output(*args, end='\n') -> None:
    if len(args) == 0:
        pass
    elif len(args) > 1 or isinstance(args[0], str):
        lines = ' '.join(
            [arg if isinstance(arg, str) else str(arg) for arg in args])
        sys.stdout.buffer.write(lines.encode('utf-8'))
    else:
        sys.stdout.buffer.write(args[0])
    if len(end) > 0:
        sys.stdout.buffer.write(end.encode('utf-8'))


class FileRevision:
    def __init__(self, path: str, rev: str, state: str, markseq: int) -> None:
        self.path = path
        self.rev = rev
        self.state = state
        self.markseq = markseq


class ChangeSetKey:
    def __init__(
            self,
            branch: str,
            author,
            timestamp: int,
            log: bytes,
            commitid: Optional[str],
            fuzzsec: int
    ) -> None:
        self.branch = branch
        self.author = author
        self.min_time = timestamp
        self.max_time = timestamp
        self.commitid = commitid
        self.fuzzsec = fuzzsec
        self.revs: List[FileRevision] = []
        self.tags: List[str] = []
        self.log_hash = 0
        h = 0
        for c in log:
            h = 31 * h + c
        self.log_hash = h

    def __lt__(self, other) -> bool:
        return self._cmp(other) < 0

    def __gt__(self, other) -> bool:
        return self._cmp(other) > 0

    def __eq__(self, other) -> bool:
        return self._cmp(other) == 0

    def __le__(self, other) -> bool:
        return self._cmp(other) <= 0

    def __ge__(self, other) -> bool:
        return self._cmp(other) >= 0

    def __ne__(self, other) -> bool:
        return self._cmp(other) != 0

    def _cmp(self, anon) -> int:
        if not isinstance(anon, ChangeSetKey):
            raise TypeError()
        # compare by the commitid
        cid = _cmp2(self.commitid, anon.commitid)
        if cid == 0 and self.commitid is not None:
            # both have commitid and they are same
            return 0

        # compare by the time
        ma = anon.min_time - self.max_time
        mi = self.min_time - anon.max_time
        ct = self.min_time - anon.min_time
        if ma > self.fuzzsec or mi > self.fuzzsec:
            return ct

        if cid != 0:
            # only one has the commitid, this means different commit
            return cid if ct == 0 else ct

        # compare by log, branch and author
        c = _cmp2(self.log_hash, anon.log_hash)
        if c == 0:
            c = _cmp2(self.branch, anon.branch)
        if c == 0:
            c = _cmp2(self.author, anon.author)
        if c == 0:
            return 0

        return ct if ct != 0 else c

    def merge(self, anot: "ChangeSetKey") -> None:
        self.max_time = max(self.max_time, anot.max_time)
        self.min_time = min(self.min_time, anot.min_time)
        self.revs.extend(anot.revs)

    def __hash__(self) -> int:
        return hash(self.branch + '/' + self.author) * 31 + self.log_hash

    def put_file(self, path: str, rev: str, state: str, markseq: int):
        self.revs.append(FileRevision(path, rev, state, markseq))


TCmp = TypeVar("TCmp", int, str)
def _cmp2(a: Optional[TCmp], b: Optional[TCmp]) -> int:
    _a = a is not None
    _b = b is not None
    return (a > b) - (a < b) if _a and _b else (_a > _b) - (_a < _b)  # type: ignore


class CvsConv:
    def __init__(self, cvsroot: str, rcs: "RcsKeywords", dumpfile: bool, fuzzsec: int) -> None:
        self.cvsroot = cvsroot
        self.rcs = rcs
        self.changesets: Dict[ChangeSetKey, ChangeSetKey] = dict()
        self.dumpfile = dumpfile
        self.markseq = 0
        self.tags: Dict[str, ChangeSetKey] = dict()
        self.fuzzsec = fuzzsec

    def walk(self, module: Optional[str] =None) -> None:
        p = [self.cvsroot]
        if module is not None:
            p.append(module)
        path = os.path.join(*p)

        for root, dirs, files in os.walk(path):
            if '.git' in dirs:
                print('Ignore %s: cannot handle the path named \'.git\'' % (
                      root + os.sep + '.git'), file=sys.stderr)
                dirs.remove('.git')
            if '.git' in files:
                print('Ignore %s: cannot handle the path named \'.git\'' % (
                      root + os.sep + '.git'), file=sys.stderr)
                files.remove('.git')
            for f in files:
                if not f[-2:] == ',v':
                    continue
                self.parse_file(root + os.sep + f)

        for t, c in list(self.tags.items()):
            c.tags.append(t)

    def parse_file(self, path: str) -> None:
        rtags: Dict[str, List[str]] = dict()
        rcsfile = rcsparse.rcsfile(path)
        branches = {'1': 'HEAD', '1.1.1': 'VENDOR'}
        for k, v_ in list(rcsfile.symbols.items()):
            r = v_.split('.')
            if len(r) == 3:
                branches[v_] = 'VENDOR'
            elif len(r) >= 3 and r[-2] == '0':
                branches['.'.join(r[:-2] + r[-1:])] = k
            if len(r) == 2 and branches[r[0]] == 'HEAD':
                if v_ not in rtags:
                    rtags[v_] = list()
                rtags[v_].append(k)

        revs: List[Tuple[str, Tuple[str, int, str, str, List[str], str, str]]] = list(rcsfile.revs.items())
        # sort by revision descending to priorize 1.1.1.1 than 1.1
        revs.sort(key=lambda a: a[1][0], reverse=True)
        # sort by time
        revs.sort(key=lambda a: a[1][1])
        novendor = False
        have_initial_revision = False
        last_vendor_status = None
        for k, v in revs:
            r = k.split('.')
            if len(r) == 4 and r[0] == '1' and r[1] == '1' and r[2] == '1' \
                    and r[3] == '1':
                if have_initial_revision:
                    continue
                if v[3] == 'dead':
                    continue
                last_vendor_status = v[3]
                have_initial_revision = True
            elif len(r) == 4 and r[0] == '1' and r[1] == '1' and r[2] == '1':
                if novendor:
                    continue
                last_vendor_status = v[3]
            elif len(r) == 2:
                if r[0] == '1' and r[1] == '1':
                    if have_initial_revision:
                        continue
                    if v[3] == 'dead':
                        continue
                    have_initial_revision = True
                elif r[0] == '1' and r[1] != '1':
                    novendor = True
                if last_vendor_status == 'dead' and v[3] == 'dead':
                    last_vendor_status = None
                    continue
                last_vendor_status = None
            else:
                # trunk only
                continue

            if self.dumpfile:
                self.markseq = self.markseq + 1
                git_dump_file(path, k, self.rcs, self.markseq)

            b = '.'.join(r[:-1])
            try:
                a = ChangeSetKey(
                    branches[b], v[2], v[1], rcsfile.getlog(v[0]), v[6],
                    self.fuzzsec)
            except Exception as e:
                print('Aborted at %s %s' % (path, v[0]), file=sys.stderr)
                raise e

            a.put_file(path, k, v[3], self.markseq)
            while a in self.changesets:
                c = self.changesets[a]
                del self.changesets[a]
                c.merge(a)
                a = c
            self.changesets[a] = a
            if k in rtags:
                for t in rtags[k]:
                    if t not in self.tags or \
                            self.tags[t].max_time < a.max_time:
                        self.tags[t] = a


def file_path(r: str, p: str) -> str:
    if r.endswith('/'):
        r = r[:-1]
    if p[-2:] == ',v':
        path = p[:-2]               # drop ",v"
    else:
        path = p
    p_ = path.split('/')
    if len(p_) > 0 and p_[-2] == 'Attic':
        path = '/'.join(p_[:-2] + [p_[-1]])
    if path.startswith(r):
        path = path[len(r) + 1:]
    return path


def git_dump_file(path: str, k, rcs, markseq) -> None:
    try:
        cont = rcs.expand_keyword(path, rcsparse.rcsfile(path), k)
    except RuntimeError as msg:
        print('Unexpected runtime error on parsing',
              path, k, ':', msg, file=sys.stderr)
        print('unlimit the resource limit may fix this problem.',
              file=sys.stderr)
        sys.exit(1)
    output('blob')
    output('mark :%d' % markseq)
    output('data', len(cont))
    output(cont)


class RcsKeywords:
    RCS_KW_AUTHOR   = (1 << 0)
    RCS_KW_DATE     = (1 << 1)
    RCS_KW_LOG      = (1 << 2)
    RCS_KW_NAME     = (1 << 3)
    RCS_KW_RCSFILE  = (1 << 4)
    RCS_KW_REVISION = (1 << 5)
    RCS_KW_SOURCE   = (1 << 6)
    RCS_KW_STATE    = (1 << 7)
    RCS_KW_FULLPATH = (1 << 8)
    RCS_KW_MDOCDATE = (1 << 9)
    RCS_KW_LOCKER   = (1 << 10)

    RCS_KW_ID       = (RCS_KW_RCSFILE | RCS_KW_REVISION | RCS_KW_DATE |
                       RCS_KW_AUTHOR | RCS_KW_STATE)
    RCS_KW_HEADER   = (RCS_KW_ID | RCS_KW_FULLPATH)

    rcs_expkw = {
        b"Author":   RCS_KW_AUTHOR,
        b"Date":     RCS_KW_DATE,
        b"Header":   RCS_KW_HEADER,
        b"Id":       RCS_KW_ID,
        b"Log":      RCS_KW_LOG,
        b"Name":     RCS_KW_NAME,
        b"RCSfile":  RCS_KW_RCSFILE,
        b"Revision": RCS_KW_REVISION,
        b"Source":   RCS_KW_SOURCE,
        b"State":    RCS_KW_STATE,
        b"Mdocdate": RCS_KW_MDOCDATE,
        b"Locker":   RCS_KW_LOCKER
    }

    RCS_KWEXP_NONE    = (1 << 0)
    RCS_KWEXP_NAME    = (1 << 1)    # include keyword name
    RCS_KWEXP_VAL     = (1 << 2)    # include keyword value
    RCS_KWEXP_LKR     = (1 << 3)    # include name of locker
    RCS_KWEXP_OLD     = (1 << 4)    # generate old keyword string
    RCS_KWEXP_ERR     = (1 << 5)    # mode has an error
    RCS_KWEXP_DEFAULT = (RCS_KWEXP_NAME | RCS_KWEXP_VAL)
    RCS_KWEXP_KVL     = (RCS_KWEXP_NAME | RCS_KWEXP_VAL | RCS_KWEXP_LKR)

    def __init__(self) -> None:
        self.rerecomple()

    def rerecomple(self) -> None:
        pat = b'|'.join(list(self.rcs_expkw.keys()))
        self.re_kw = re.compile(b".*?\\$(" + pat + b")[\\$:]")

    def add_id_keyword(self, keyword) -> None:
        self.rcs_expkw[keyword.encode('ascii')] = self.RCS_KW_ID
        self.rerecomple()

    def kflag_get(self, flags: Optional[str]) -> int:
        if flags is None:
            return self.RCS_KWEXP_DEFAULT
        fl = 0
        for fc in flags:
            if fc == 'k':
                fl |= self.RCS_KWEXP_NAME
            elif fc == 'v':
                fl |= self.RCS_KWEXP_VAL
            elif fc == 'l':
                fl |= self.RCS_KWEXP_LKR
            elif fc == 'o':
                if len(flags) != 1:
                    fl |= self.RCS_KWEXP_ERR
                fl |= self.RCS_KWEXP_OLD
            elif fc == 'b':
                if len(flags) != 1:
                    fl |= self.RCS_KWEXP_ERR
                fl |= self.RCS_KWEXP_NONE
            else:
                fl |= self.RCS_KWEXP_ERR
        return fl

    def expand_keyword(self, filename: str, rcs: rcsparse.rcsfile, r: str) -> bytes:
        """
        Check out a file with keywords expanded. Expansion rules are specific
        to each keyword, and some cases specific to undocumented behaviour of CVS.
        Our implementation does not expand some keywords (see comments in the code).
        For a list of keywords and their expansion rules, see:
        https://www.gnu.org/software/trans-coord/manual/cvs/cvs.html#Keyword-list
        (also available in 'info cvs' if cvs is installed)
        """
        rev = rcs.revs[r]

        mode = self.kflag_get(rcs.expand)
        if (mode & (self.RCS_KWEXP_NONE | self.RCS_KWEXP_OLD)) != 0:
            return rcs.checkout(rev[0])

        ret = []
        for line in rcs.checkout(rev[0]).splitlines(keepends=True):
            logbuf = None
            m = self.re_kw.match(line)
            if m is None:
                # No RCS Keywords, use it as it is
                ret.append(line)
                continue

            line0 = b''
            while m is not None:
                logbuf = None
                try:
                    dsign = m.end(1) + line[m.end(1):].index(b'$')
                except ValueError:
                    # No RCS Keywords, use it as it is
                    ret.append(line)
                    break
                prefix = line[:m.start(1) - 1]
                next_match_segment = copy.deepcopy(line[dsign:])
                line = line[dsign + 1:]
                expbuf = ''
                if (mode & self.RCS_KWEXP_NAME) != 0:
                    expbuf += '$'
                    expbuf += m.group(1).decode('ascii')
                    if (mode & self.RCS_KWEXP_VAL) != 0:
                        expbuf += ': '
                if (mode & self.RCS_KWEXP_VAL) != 0:
                    expkw = self.rcs_expkw[m.group(1)]
                    if (expkw & self.RCS_KW_RCSFILE) != 0:
                        expbuf += filename \
                            if (expkw & self.RCS_KW_FULLPATH) != 0 \
                            else os.path.basename(filename)
                        expbuf += " "
                    if (expkw & self.RCS_KW_REVISION) != 0:
                        expbuf += rev[0]
                        expbuf += " "
                    if (expkw & self.RCS_KW_DATE) != 0:
                        expbuf += time.strftime(
                            "%Y/%m/%d %H:%M:%S ", time.gmtime(rev[1]))
                    if (expkw & self.RCS_KW_MDOCDATE) != 0:
                        d = time.gmtime(rev[1])
                        expbuf += time.strftime(
                            "%B%e %Y " if (d.tm_mday < 10) else "%B %e %Y ", d)
                    if (expkw & self.RCS_KW_AUTHOR) != 0:
                        expbuf += rev[2]
                        expbuf += " "
                    if (expkw & self.RCS_KW_STATE) != 0:
                        expbuf += rev[3]
                        expbuf += " "
                    if (expkw & self.RCS_KW_LOG) != 0:
                        # Unlike other keywords, the Log keyword expands over multiple lines.
                        # The terminating '$' of the Log keyword appears on the line which
                        # contains the log keyword itself. Then follow all log message lines,
                        # and those lines are followed by content which follows the Log keyword.
                        # For example, the line:
                        #
                        #    $Log$ content which follows
                        #
                        # must be expanded like this:
                        #
                        #   $Log: delta,v $
                        #   Revision 1.2  2021/11/29 14:24:18  stsp
                        #   log message line 1
                        #   log message line 2
                        #   content which follows
                        #
                        # If we did not trim the Log keyword's trailing "$" here then
                        # the last line would read instead:
                        #
                        #   $ content which follows
                        assert(next_match_segment[0] == ord('$'))
                        next_match_segment = next_match_segment[1:]
                        p = prefix
                        expbuf += filename \
                            if (expkw & self.RCS_KW_FULLPATH) != 0 \
                            else os.path.basename(filename)
                        expbuf += " "
                        logbuf = p + (
                            'Revision %s  %s  %s\n' % (
                                rev[0], time.strftime(
                                    "%Y/%m/%d %H:%M:%S", time.gmtime(rev[1])),
                                rev[2])).encode('ascii')
                        for lline in rcs.getlog(rev[0]).splitlines(keepends=True):
                            logbuf += p + lline
                    if (expkw & self.RCS_KW_SOURCE) != 0:
                        expbuf += filename
                        expbuf += " "
                    if (expkw & (self.RCS_KW_NAME | self.RCS_KW_LOCKER)) != 0:
                        # We do not expand Name and Locker keywords.
                        # The Name keyword is only expanded when a file is checked
                        # out with an explicit tag name .perhaps this will be needed
                        # if the loader learns about CVS tags some day.
                        # The Locker keyword only expands if the file is currently
                        # locked via 'cvs admin -l', which is not part of the
                        # information we want to preserve about source code.
                        expbuf += " "
                if (mode & self.RCS_KWEXP_NAME) != 0:
                    expbuf += '$'
                if logbuf is not None:
                    ret.append(prefix + expbuf.encode('ascii') + b'\n' + logbuf)
                else:
                    line0 += prefix + expbuf[:255].encode('ascii')
                m = self.re_kw.match(next_match_segment)
                if m:
                    line = next_match_segment
                    if (mode & self.RCS_KWEXP_NAME) != 0 and (expkw & self.RCS_KW_LOG) == 0 and line0[-1] == ord('$'):
                        # There is another keyword on this line that needs expansion.
                        # Avoid a double "$$" in the expanded string. This $ terminates
                        # the previous keyword and marks the beginning of the next one.
                        line0 = line0[:-1]
                elif logbuf is not None:
                    # Trim whitespace from the beginning of text following the Log keyword.
                    # But leave a lone trailing empty line as-is. Which seems inconsistent,
                    # but testing suggests that this matches CVS's behaviour.
                    if len(line) == 1 and line[0] == ord('\n'):
                        ret.append(line0 + prefix + line)
                    else:
                        ret.append(line0 + prefix + line.lstrip())
                else:
                    ret.append(line0 + line)
        return b''.join(ret)


# ----------------------------------------------------------------------
# entry point
# ----------------------------------------------------------------------
if __name__ == '__main__':
    main()
