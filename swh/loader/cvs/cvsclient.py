# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Minimal CVS client implementation

"""

import socket
import subprocess
import os.path
import tempfile
import re

from swh.loader.exception import NotFound

CVS_PSERVER_PORT = 2401
CVS_PROTOCOL_BUFFER_SIZE = 8192
EXAMPLE_PSERVER_URL = "pserver://user:password@cvs.example.com/cvsroot/repository"
EXAMPLE_SSH_URL = "ssh://user@cvs.example.com/cvsroot/repository"

VALID_RESPONSES = [ "ok",  "error", "Valid-requests", "Checked-in",
    "New-entry", "Checksum", "Copy-file", "Updated", "Created",
    "Update-existing", "Merged", "Patched", "Rcs-diff", "Mode",
    "Removed", "Remove-entry", "Template", "Notified", "Module-expansion",
    "Wrapper-rcsOption", "M", "Mbinary", "E", "F", "MT" ]

# Trivially encode strings to protect them from innocent eyes (i.e.,
# inadvertent password compromises, like a network administrator
# who's watching packets for legitimate reasons and accidentally sees
# the password protocol go by).
#
# This is NOT secure encryption.
def scramble_password(password):
    s = ['A'] # scramble scheme version number
    scramble_shifts = [
        0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
       16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
      114,120, 53, 79, 96,109, 72,108, 70, 64, 76, 67,116, 74, 68, 87,
      111, 52, 75,119, 49, 34, 82, 81, 95, 65,112, 86,118,110,122,105,
       41, 57, 83, 43, 46,102, 40, 89, 38,103, 45, 50, 42,123, 91, 35,
      125, 55, 54, 66,124,126, 59, 47, 92, 71,115, 78, 88,107,106, 56,
       36,121,117,104,101,100, 69, 73, 99, 63, 94, 93, 39, 37, 61, 48,
       58,113, 32, 90, 44, 98, 60, 51, 33, 97, 62, 77, 84, 80, 85,223,
      225,216,187,166,229,189,222,188,141,249,148,200,184,136,248,190,
      199,170,181,204,138,232,218,183,255,234,220,247,213,203,226,193,
      174,172,228,252,217,201,131,230,197,211,145,238,161,179,160,212,
      207,221,254,173,202,146,224,151,140,196,205,130,135,133,143,246,
      192,159,244,239,185,168,215,144,139,165,180,157,147,186,214,176,
      227,231,219,169,175,156,206,198,129,164,150,210,154,177,134,127,
      182,128,158,208,162,132,167,209,149,241,153,251,237,236,171,195,
      243,233,253,240,194,250,191,155,142,137,245,235,163,242,178,152 ]
    for c in password:
        s.append('%c' % scramble_shifts[ord(c)])
    return "".join(s)


class CVSProtocolError(Exception):
    pass

_re_kb_opt = re.compile(b'\/-kb\/')

class CVSClient:

    def connect_pserver(self, hostname, port, auth):
        if port == None:
            port = CVS_PSERVER_PORT
        if auth == None:
            raise NotFound("Username and password are required for a pserver connection: %s" % EXAMPLE_PSERVER_URL)
        try:
          user = auth.split(':')[0]
          password = auth.split(':')[1]
        except IndexError:
            raise NotFound("Username and password are required for a pserver connection: %s" % EXAMPLE_PSERVER_URL)

        try:
          self.socket = socket.create_connection((hostname, port))
        except ConnectionRefusedError:
            raise NotFound("Could not connect to %s:%s", hostname, port)

        scrambled_password = scramble_password(password)
        request = "BEGIN AUTH REQUEST\n%s/%s\n%s\n%s\nEND AUTH REQUEST\n" \
            % (self.cvsroot_path, self.cvs_module_name, user, scrambled_password)
        self.socket.sendall(request.encode('UTF-8'))

        response = self.socket.recv(11)
        if response != b"I LOVE YOU\n":
            raise NotFound("pserver authentication failed for %s:%s" % (hostname, port))

    def connect_ssh(self, hostname, port, auth):
        command = [ 'ssh' ]
        if auth != None:
            # Assume 'auth' contains only a user name.
            # We do not support password authentication with SSH since the
            # anoncvs user is usually granted access without a password.
            command += [ '-l' , '%s' % auth ]
        if port != None:
            command += [ '-p' , '%d' % port ]

        # accept new SSH hosts keys upon first use; changed host keys will require intervention
        command += ['-o', "StrictHostKeyChecking=accept-new" ]

        # disable interactive prompting
        command += ['-o', "BatchMode=yes" ]

        # disable further option processing by adding '--'
        command += [ '--' ]

        command += ['%s' % hostname, 'cvs', 'server']
        self.ssh = subprocess.Popen(command,
            bufsize=0, # use non-buffered I/O to match behaviour of self.socket
            stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def connect_fake(self, hostname, port, auth):
        command = [ 'cvs', 'server'  ]
        self.ssh = subprocess.Popen(command,
            bufsize=0, # use non-buffered I/O to match behaviour of self.socket
            stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def conn_read_line(self, require_newline=True):
        if len(self.linebuffer) != 0:
            return self.linebuffer.pop(0)
        buf = b''
        idx = -1
        while idx == -1:
            if len(buf) >= CVS_PROTOCOL_BUFFER_SIZE:
                if require_newline:
                    raise CVSProtocolError("Overlong response from CVS server: %s" % buf)
                else:
                    break
            if self.socket:
                buf += self.socket.recv(CVS_PROTOCOL_BUFFER_SIZE)
            elif self.ssh:
                buf += self.ssh.stdout.read(CVS_PROTOCOL_BUFFER_SIZE)
            else:
                raise Exception("No valid connection")
            if not buf:
                return None
            idx = buf.rfind(b'\n')
        if idx != -1:
            self.linebuffer = buf[:idx + 1].splitlines(keepends=True)
        else:
            if require_newline:
                raise CVSProtocolError("Invalid response from CVS server: %s" % buf)
            else:
                self.linebuffer.append(buf)
        if len(self.incomplete_line) > 0:
            self.linebuffer[0] = self.incomplete_line + self.linebuffer[0]
        if idx != -1:
            self.incomplete_line = buf[idx + 1:]
        else:
            self.incomplete_line = b''
        return self.linebuffer.pop(0)

    def conn_write(self, data):
        if self.socket:
            return self.socket.sendall(data)
        if self.ssh:
            self.ssh.stdin.write(data)
            return self.ssh.stdin.flush()
        raise Exception("No valid connection")

    def conn_write_str(self, s):
        return self.conn_write(s.encode('UTF-8'))

    def conn_close(self):
        if self.socket:
            self.socket.close()
        if self.ssh:
            self.ssh.kill()
            try:
              self.ssh.wait(timeout=10)
            except TimeoutExpired as e:
              raise TimeoutExpired("Could not terminate ssh program: %s" % e)

    def __init__(self, url):
        """
        Connect to a CVS server at the specified URL and perform the initial
        CVS protocol handshake.
        """
        self.hostname = url.host
        self.cvsroot_path = os.path.dirname(url.path)
        self.cvs_module_name = os.path.basename(url.path)
        self.socket = None
        self.ssh = None
        self.linebuffer = list()
        self.incomplete_line = b''

        if url.scheme == 'pserver':
            self.connect_pserver(url.host, url.port, url.auth)
        elif url.scheme == 'ssh':
            self.connect_ssh(url.host, url.port, url.auth)
        elif url.scheme == 'fake':
            self.connect_fake(url.host, url.port, url.auth)
        else:
            raise NotFound("Invalid CVS origin URL '%s'" % url)

        # we should have a connection now
        assert self.socket or self.ssh

        self.conn_write_str("Root %s\nValid-responses %s\nvalid-requests\nUseUnchanged\n" % \
            (self.cvsroot_path, ' '.join(VALID_RESPONSES)))
        response = self.conn_read_line()
        if not response:
            raise CVSProtocolError("No response from CVS server")
        try:
            if response[0:15] != b"Valid-requests ":
                raise CVSProtocolError("Invalid response from CVS server: %s" % response)
        except IndexError:
            raise CVSProtocolError("Invalid response from CVS server: %s" % response)
        response = self.conn_read_line()
        if response != b"ok\n":
            raise CVSProtocolError("Invalid response from CVS server: %s" % response)

    def __del__(self):
        self.conn_close()

    def _parse_rlog_response(self, fp):
        rlog_output = tempfile.TemporaryFile()
        expect_error = False
        for line in fp.readlines():
            if expect_error:
                raise CVSProtocolError('CVS server error: %s' % line)
            if line == b'ok\n':
                break
            elif line == b'M \n':
                continue
            elif line[0:2] == b'M ':
                rlog_output.write(line[2:])
            elif line[0:8] == b'MT text ':
                rlog_output.write(line[8:-1])
            elif line[0:8] == b'MT date ':
                rlog_output.write(line[8:-1])
            elif line[0:10] == b'MT newline':
                rlog_output.write(line[10:])
            elif line[0:7] == b'error  ':
                epxect_error = True
                continue
            else:
                raise CVSProtocolError('Bad CVS protocol response: %s' % line)
        rlog_output.seek(0)
        return rlog_output


    def fetch_rlog(self):
        fp = tempfile.TemporaryFile()
        self.conn_write_str("Global_option -q\nArgument --\nArgument %s\nrlog\n" % \
            self.cvs_module_name)
        while True:
            response = self.conn_read_line()
            if response == None:
                raise CVSProtocolError("No response from CVS server")
            if response[0:2] == b"E ":
                raise CVSProtocolError("Error response from CVS server: %s" % response)
            fp.write(response)
            if response == b"ok\n":
                break
        fp.seek(0)
        return self._parse_rlog_response(fp)

    def checkout(self, path, rev, dest_dir):
        skip_line = False
        expect_modeline = False
        expect_bytecount = False
        have_bytecount = False
        bytecount = 0
        dirname = os.path.dirname(path)
        if dirname:
            self.conn_write_str("Directory %s\n%s\n" % (dirname, dirname))
        filename = os.path.basename(path)
        co_output = tempfile.NamedTemporaryFile(dir=dest_dir, delete=True,
            prefix='cvsclient-checkout-%s-r%s-' % (filename, rev))
        # TODO: cvs <= 1.10 servers expect to be given every Directory along the path.
        self.conn_write_str("Directory %s\n%s\n"
            "Global_option -q\n"
            "Argument -r%s\n"
            "Argument -kb\n"
            "Argument --\nArgument %s\nco \n" % (self.cvs_module_name,
            self.cvs_module_name, rev, path))
        while True:
            if have_bytecount and bytecount > 0:
                response = self.conn_read_line(require_newline=False)
                if response == None:
                    raise CVSProtocolError("Incomplete response from CVS server")
                co_output.write(response)
                bytecount -= len(response)
                if bytecount < 0:
                    raise CVSProtocolError("Overlong response from CVS server: %s" % response)
                continue
            else:
                response = self.conn_read_line()
            if response[0:2] == b'E ':
                raise CVSProtocolError('Error from CVS server: %s' % response)
            if have_bytecount and bytecount == 0 and response == b'ok\n':
                break
            if skip_line:
                skip_line = False
                continue
            elif expect_bytecount:
                try:
                    bytecount = int(response[0:-1]) # strip trailing \n
                except ValueError:
                    raise CVSProtocolError('Bad CVS protocol response: %s' % response)
                have_bytecount = True
                continue
            elif response == b'M \n':
                continue
            elif response == b'MT +updated\n':
                continue
            elif response == b'MT -updated\n':
                continue
            elif response[0:9] == b'MT fname ':
                continue
            elif response[0:8] == b'Created ':
                skip_line = True
                continue
            elif response[0:1] == b'/' and _re_kb_opt.search(response):
                expect_modeline = True
                continue
            elif expect_modeline and response[0:2] == b'u=':
                expect_modeline = False
                expect_bytecount = True
                continue
            elif response[0:2] == b'M ':
                continue
            elif response[0:8] == b'MT text ':
                continue
            elif response[0:10] == b'MT newline':
                continue
            else:
                raise CVSProtocolError('Bad CVS protocol response: %s' % response)
        co_output.seek(0)
        return co_output