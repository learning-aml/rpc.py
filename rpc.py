# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2016 trueEX Group, LLC

"""RPC over SSH support module.

This module provides a basic, dynamic RPC over SSH mechanism to Python
code.  It does not require deployment to endpoints; instead, you
configure it with a script that will be copied to each endpoint on
demand and launched as an RPC endpoint.

The module assumes you have created a ZIP file of the Python code that
you would like to run on the RPC endpoints.  You should set the
`ZIPNAME' module property to the name of this executable.  The
executable should accept a `-r' option that causes it to enter the
server_loop() function below, implying that this module must be
included in the executable.  It is perfectly acceptable (and expected)
that `ZIPNAME' will point to the same script that implements the
client functionality.

add() and multiadd() are used to associate an endpoint name with an
actual hostname and perform the initial SSH connection.  remove() and
remove_all() will clean up one or all endpoints and close the relevant
SSH connection(s).

call() and multicall() perform RPC requests against endpoints.
multicall() generalizes call() by starting up a thread per endpoint,
each running call() in parallel and blocking until the results from
all endpoints are collected.

hostspec() and callspec() are used to configure multiadd() and
multicall() function calls, respectively.

set_connect_timeout(), set_default_timeout(), and set_next_timeout()
are used for SSH connection and RPC timeout management.
enable_next_debug() can be used to repeat remote stdout/stderr locally
for debugging purposes.

server_loop() runs a loop that accepts requests over standard input
and responds with the answer over standard error.  See `RPC Protocol
Description' below.


RPC Protocol Description
========================

Requests are bundled into a 4-tuple in the following format:

  (name, function, args, kwargs, debug)

    `name' is a user-defined name that uniquely identifies this RPC
    endpoint.

    `function' is the function to run.

    `args' is a tuple of arguments.

    `kwargs' is a dict() of keyword arguments.

    `debug' is used to indicate whether to capture and forward remote
    stdout/stderr

Responses are bundled into a 2-tuple with one of the following forms:

  ('ok', result)

  ('raise', exc)

    'ok' and 'raise' are strings.

    If the function did not raise an exception, the 'ok' form is
    returned and `result' is the function's return value.

    If the function raised an exception, the 'raise' form is returned
    and `exc' is the exception.

Request and response tuples are encoded using the `dill' library, and
remote stdout/stderr are sent as a byte stream.  Requests, responses,
and the debugging byte stream are framed using a simple framing format
meant to make communication over SSH simple to implement:

   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                     Request/Response Size                     |
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                             Data                              |
  :                                                               :

  `Request Size' is an unsigned 32-bit integer in network byte order
  (big-endian).  This limits the data size to 4,294,967,295 bytes.  A
  request size of 0 indicates that the remote server loop should
  terminate.

  `Data' is the request itself, encoded using the `dill' library in
  the case of a request or response tuple.

"""

import os
import sys
import traceback
import struct
import threading
import atexit
import socket
import logging
from collections import deque
from contextlib import contextmanager

import subprocess  # see _exception_fixup() below

import paramiko
import dill

logger = logging.getLogger(__name__)

ZIPNAME = 'deploy'


#
# ENDPOINT MANAGEMENT API
#

def add(name, host, user=None, key_file=None):
    """Add an endpoint to the RPC system.

    `name' is a string that will represent this endpoint in subsequent
    calls.  `host' is the actual hostname.

    SSH user and key file specifications have the following precedence
    (first match wins):

      - `user' and `key_file' named parameters

      - `User' and `IdentityFile' parameters matching the specified
        host in ~/.ssh/config

      - Current user and default SSH private key

    """
    _ssh_mgr().add(name, host, user=user, key_file=key_file)

def remove(name):
    """Remove an endpoint from the RPC system.

    Calling remove() cleans up the endpoint's remote working directory
    and closes its SSH connection.

    """
    _ssh_mgr().remove(name)

def exists(name):
    """Report if the given endpoint exists."""
    return name in _ssh_mgr()

def hostspec(host, user=None, key_file=None):
    """Generate a hostspec for use with multiadd()."""
    return host, (), {'user': user, 'key_file': key_file}

def multiadd(hostspecs):
    """Add multiple endpoints to the RPC system in parallel.

    `hostspecs' should be a dict() associating each endpoint name with
    a hostspec generated using the hostspec() function:

      import rpc
      ...
      hspecs = dict()
      hspecs['name1'] = rpc.hostspec('host1.example.com', user='dev')
      hspecs['name2'] = rpc.hostspec('host2.example.com')
      hspecs['name3'] = rpc.hostspec('host3.example.com', user='qa',
                                      key_file='~/.ssh/id_rsa.qa')
      rpc.multiadd(hspecs)

    Exceptions raised while connecting to SSH servers will be
    collected into a single MultiException, which is then raised after
    all connection attempts finish with success or failure.  These
    exceptions may be accessed via the dict() referenced by the
    `exceptions' property of the MultiException instance.

    SSH user and key file specifications have the following precedence
    (first match wins):

      - `user' and `key_file' named parameters to hostspec()

      - `User' and `IdentityFile' parameters matching the specified
        host in ~/.ssh/config

      - Current user and default SSH private key

    """
    _ssh_mgr().multiadd(hostspecs)

def shutdown():
    """Remove all endpoints from the RPC system.

    Calling shutdown() cleans up each endpoint's remote working
    directory and closes all SSH connections.

    """
    _ssh_mgr().remove_all()


#
# RPC CLIENTS AND SERVER API
#

def set_connect_timeout(timeout):
    """Set connection timeout to `timeout' for add() and multiadd()."""
    _ssh_mgr().connect_timeout = timeout

def set_default_timeout(name, timeout):
    """Set default timeout to `timeout' for RPC calls to endpoint `name'."""
    cstate = _ssh_mgr()[name]
    cstate.default_timeout = timeout

def set_next_timeout(name, timeout):
    """Set timeout to `timeout' for next RPC call to endpoint `name'.

    Timeout will be reset to the default after this endpoint is next
    used via call() or multicall().  This is designed to encourage
    small default timeouts while still enabling call-specific
    timeouts, without complicating the call() and multicall() APIs.

    """
    cstate = _ssh_mgr()[name]
    cstate.next_timeout = timeout

def enable_next_debug(name):
    """Enable debugging for next RPC call to endpoint `name'.

    When debugging is enabled, stdout and stderr from the remote
    Python process is collected and output to the caller's stdout.
    Note that the output is not printed to the caller's tty as it
    happens; instead, all output is retrieved and printed when the RPC
    call returns.  This mechanism is likely not suited for large
    volumes of data.

    Debug state will be reset to False after this endpoint is next
    used via call() or multicall().

    """
    cstate = _ssh_mgr()[name]
    cstate.next_debug = True

def call(name, function, *args, **kwargs):
    """Call function(*args, **kwargs) on remote endpoint `name'.

    Remote exceptions will be raised in the caller's context.

    """
    try:
        return _call(name, function, *args, **kwargs)
    except socket.timeout:
        # Shut down SSH connection in case of socket timeout.  The
        # remove() will either succeed because SSH is fine and our
        # function simply did not return in time, or it will raise
        # another exception because the connection itself is having
        # trouble.  This is desirable.
        cstate = _ssh_mgr()[name]
        cstate.session.close()
        remove(name)
        raise

def _call(name, function, *args, **kwargs):
    # pylint: disable=too-many-locals

    # Find SSH channel, configure timeout and debug mode
    cstate = _ssh_mgr()[name]
    session = cstate.session
    session.settimeout(cstate.next_timeout)
    cstate.next_timeout = cstate.default_timeout
    debug = cstate.next_debug
    cstate.next_debug = False

    # Serialize our function and arguments
    msg = name, function, args, kwargs, debug
    msg_bin = dill.dumps(msg)

    # Send size and request to endpoint
    session.sendall(struct.pack('>I', len(msg_bin)))
    session.sendall(msg_bin)

    # Relay remote stdout (will be length 0 unless debug mode is
    # enabled)
    debug_size, = struct.unpack('>I', ssh_readn(session, 4))
    stdout = ''
    if debug_size > 0:
        stdout = ssh_readn(session, debug_size)
        sys.stdout.write(stdout)

    # Read size and response from endpoint via stderr
    size, = struct.unpack('>I', ssh_readn(session, 4, stderr=True))
    ssh_readn(session, size, stderr=True)  # returns 'result'

    # Read result file over sftp
    with cstate.client.open_sftp() as sftp:
        result_path = os.path.join(cstate.top_dir, 'result')
        with sftp.file(result_path, 'r') as result_file:
            reply_bin = result_file.read()

    # Deserialize reply and handle
    reply = dill.loads(reply_bin)
    if reply[0] == 'ok':
        result = reply[1]
    elif reply[0] == 'raise':
        exc = reply[1]
        raise exc

    return result

def callspec(function, *args, **kwargs):
    """Generate a callspec using function calling conventions."""
    return function, args, kwargs

def multicall(callspecs):
    """Call functions on multiple remote endpoints.

    Run call() against multiple endpoints, each its own thread.  The
    function returns when all endpoints have replied.

    `callspecs' should be a dict() associating each endpoint name with
    a callspec generated using the callspec() function:

      import rpc
      ...
      # Endpoints name1, name2, and name3 configured here...
      ...
      cspecs = dict()
      cspecs['name1'] = rpc.callspec(func1)             # no args
      cspecs['name2'] = rpc.callspec(func2, arg)        # with an argument
      cspecs['name3'] = rpc.callspec(func3, karg='foo') # keyword argument
      results = rpc.multicall(cspecs)

    While a bit complex, this configuration allows endpoints to run
    distinct commands in parallel.

    The return value is a dict() that associates each endpoint's name
    with its respective function's return value.

    Exceptions raised while running remote functions will be collected
    into a single MultiException, which is then raised on the caller
    after waiting for all threads to finish with success or failure.
    These exceptions may be accessed via the dict() referenced by the
    `exceptions' property of the MultiException instance, and
    successful results are contained in the dict() referenced by the
    `results' property.

    """

    def _do_call(name, function, args, keywords):
        return call(name, function, *args, **keywords)

    # Wrap each supplied callspec in a new callspec, which will then
    # be unwrapped by _parallel_do() below
    newspecs = dict()
    for name, spec in callspecs.iteritems():
        function, args, kwargs = spec
        newspecs[name] = callspec(_do_call, name, function, args, kwargs)

    results, failures = _parallel_do(newspecs)

    if len(failures) > 0:
        exc = MultiException(
            'Exception(s) raised during call to multicall():')
        exc.exceptions = failures
        exc.results = results
        raise exc

    return results

def server_loop():
    """Repeatedly run serialized closures in local interpeter."""
    # pylint: disable=too-many-locals

    mydir = os.path.dirname(os.path.realpath(sys.argv[0]))
    while True:
        # Read request via stdin
        size, = struct.unpack('>I', sys.stdin.read(4))
        if size == 0:
            # size of 0 indicates end of RPC mode
            break
        data = sys.stdin.read(size)
        host, function, args, kwargs, debug = dill.loads(data)

        # Redirect stdout and stderr to a file while running
        # user-supplied code.  This accomplishes the following:
        #  - Keeps stderr free to use for serialized return value
        #  - Keeps stdout from hitting SSH channel, so we don't hit
        #    any windows and possibly block the code writing to stdout
        #  - Allows us to send back all stdout/stderr prefixed with a
        #    size parameter when we are in debug mode
        os.chdir(mydir)
        with open('out', 'w') as out:
            with redirect(sys.stdout, out), \
                 redirect(sys.stderr, out):
                try:
                    result = ('ok', function(*args, **kwargs))

                except Exception, exc:  # pylint: disable=broad-except
                    exctype, value, tback = sys.exc_info()
                    exc.rpc_traceback = (exctype, value,
                                         traceback.extract_tb(tback))
                    exc.rpc_host = host
                    result = ('raise', _exception_fixup(exc))

        # In debug mode, send output back via stdout
        if not debug:
            sys.stdout.write(struct.pack('>I', 0))
        else:
            os.chdir(mydir)
            with open('out', 'r') as out:
                out.seek(0, os.SEEK_END)
                sys.stdout.write(struct.pack('>I', out.tell()))
                out.seek(0, os.SEEK_SET)
                sys.stdout.write(out.read())
        sys.stdout.flush()

        # Write result to file
        result_bin = dill.dumps(result)
        with open('result', 'w') as result_file:
            result_file.write(result_bin)

        # Send size and response back via stderr
        reply_str = 'result'
        sys.stderr.write(struct.pack('>I', len(reply_str)))
        sys.stderr.flush()
        sys.stderr.write(reply_str)
        sys.stderr.flush()

def _exception_fixup(exc):
    """Fixup exceptions that fail to implement the Exception protocol.

    Some exceptions in the standard Python library do not adhere to
    the Exception protocol.  Specifically, they do not set an `args'
    attribute that contains the parameters passed to the constructor
    (this is usually done by calling the superclass constructor).  The
    args attribute is assumed to exist by pickle and unpickling will
    fail if it does not exist.

    Two relevant links:

      - https://mail.python.org/pipermail/python-dev/2007-April/072416.html
      - https://bugs.python.org/issue1692335

    We check for `args' before fixing as an attempt to handle newer
    interpreters that contain the following fix to BaseException:

      http://hg.python.org/cpython/rev/68e2690a471d

    """
    # pylint: disable=unidiomatic-typecheck

    if type(exc) is subprocess.CalledProcessError:
        # CalledProcessError fails to call the superclass constructor
        # or otherwise set up the args attribute
        if 'args' not in exc.__dict__:
            exc.args = exc.returncode, exc.cmd, exc.output
    return exc


#
# PARALLEL THREAD LAUNCHER
#

def _parallel_do(callspecs):
    resq = deque()
    def _entry(function, name, *args, **kwargs):
        try:
            result = function(*args, **kwargs)
            resq.append(('ok', name, result))
        except Exception:  # pylint: disable=broad-except
            exctype, value, tback = sys.exc_info()
            resq.append(('exc', name, (exctype, value, tback)))

    threads = list()
    for name, value in callspecs.iteritems():
        function, args, kwargs = value
        allargs = (function, name) + args
        thr = threading.Thread(
            target=_entry, name="_parallel_do", args=allargs, kwargs=kwargs)
        threads.append(thr)
        thr.start()

    for thr in threads:
        thr.join()

    results = dict()
    failures = dict()
    for status, name, value in resq:
        if status == 'ok':
            results[name] = value

        else: # status == 'exc'
            failures[name] = value

    return results, failures


#
# SSH CONNECTION CACHING
#
# We cheat here and use an attribute on a function to maintain a
# persistent cache.  This seems odd, but it abstracts away the
# "network" of RPC hosts that are connected and allows rpc callers to
# pretend that connection setup is instant.
#

def _ssh_mgr():
    if 'mgr' not in _ssh_mgr.__dict__:
        # Create an SSHCache class and instance if this is the first run
        _ssh_mgr.mgr = _SSHManager(_hostkey_callback)
    return _ssh_mgr.mgr


#
# SSH CONNECTION SUPPORT
#

class EOFException(Exception):
    """Unexpected EOF while reading or writing SSH channel."""

def ssh_readn(channel, nbytes, stderr=False):
    nleft = nbytes
    buf = ''
    while (nleft > 0):
        if not stderr:
            recvbuf = channel.recv(nleft)
        else:
            recvbuf = channel.recv_stderr(nleft)
        nread = len(recvbuf)

        if nread == 0:
            # Unexpected EOF
            raise EOFException('EOF in SSH channel while expecting more data')

        nleft -= nread
        buf += recvbuf

    return buf


#
# SSH CONNECTION MANAGEMENT
#

def _hostkey_callback(client, hostname, key):
    # TODO: Implement host key acceptance
    # pylint: disable=unused-argument
    logger.warning('%s: TODO: Host key acceptance not implemented, continuing',
                   hostname)

class _SSHHostKeyPolicy(paramiko.client.MissingHostKeyPolicy):
    """Delegate host key handling to users of Manager class."""
    # pylint: disable=too-few-public-methods

    def __init__(self, hostkey_callback):
        self._hostkey_callback = hostkey_callback

    def missing_host_key(self, client, hostname, key):
        self._hostkey_callback(client, hostname, key)

_DEFAULT_TIMEOUT = 100

class _SSHClientState(object):
    """Data class for tracking SSH client and related settings."""
    # pylint: disable=too-many-arguments,too-few-public-methods
    # pylint: disable=too-many-instance-attributes
    def __init__(self, name, host, client, session, top_dir,
                 default_timeout=_DEFAULT_TIMEOUT,
                 next_timeout=_DEFAULT_TIMEOUT,
                 user=None, key_file=None):
        self.name = name
        self.host = host
        self.client = client
        self.session = session
        self.top_dir = top_dir
        self.default_timeout = default_timeout
        self.next_timeout = next_timeout
        self.next_debug = False
        self.user = user
        self.key_file = key_file

class MultiException(Exception):
    """Contains exception(s) raised while running multiadd() or multicall().

    For both multiadd() and multicall(), a property called
    `exceptions' is a dict() of exceptions, mapping endpoint names to
    the exception raised for each.

    For multicall() only, a property called `results' contains results
    for endpoints that completed their calls successfully.

    """
    exceptions = dict()
    results = dict()

class _SSHManager(dict):
    """Top-level interface for a set of SSH connections."""

    known_hosts = "~/.ssh/known_hosts"
    ssh_config = "~/.ssh/config"

    connect_timeout = _DEFAULT_TIMEOUT

    def __init__(self, hostkey_callback=None):
        self._hosts = dict()
        self._hostkey_callback = hostkey_callback

        self._client_config = paramiko.config.SSHConfig()
        config_path = os.path.expanduser(self.ssh_config)
        if os.path.isfile(config_path):
            config_file = open(config_path, 'r')
            self._client_config.parse(config_file)

        super(_SSHManager, self).__init__()

    def _connect(self, host, user=None, key_file=None):
        client = self._new_client()
        host_config = self._client_config.lookup(host)

        # IdentifyFile support
        if key_file is None and 'identityfile' in host_config:
            key_file = host_config['identityfile']

        # User support
        if user is None and 'user' in host_config:
            user = host_config['user']

        client.connect(host, key_filename=key_file, username=user,
                       timeout=self.connect_timeout)
        session, top_dir = _session_setup(client)
        return client, session, top_dir

    def add(self, name, host, user=None, key_file=None):
        """Set up an endpoint within this manager."""
        if name not in self:
            client, session, top_dir = self._connect(host, user, key_file)
            self[name] = _SSHClientState(name, host, client, session, top_dir,
                                         user=user, key_file=key_file)

    def remove(self, name):
        """Shut down and remove an endpoint within this manager."""
        cstate = self[name]
        _session_cleanup(cstate.client, cstate.top_dir)
        self[name].client.close()
        del self[name]

    def remove_all(self):
        """Remove all endpoints from this manager."""

        def _do_remove(name):
            self.remove(name)

        # Set up callspecs
        specs = dict()
        for name in self.keys():
            specs[name] = callspec(_do_remove, name)
        _, failures = _parallel_do(specs)

        if len(failures) > 0:
            exc = MultiException(
                'Exception(s) raised during call to remove_all():')
            exc.exceptions = failures
            raise exc

    def multiadd(self, hostspecs):
        """Call function(*args, **kwargs) on multiple remote hosts."""
        # pylint: disable=too-many-locals

        def _do_connect(host, user, key_file):
            client, session, top_dir = self._connect(host, user, key_file)
            return host, client, session, top_dir

        # Set up callspecs
        specs = dict()
        for name, hspec in hostspecs.iteritems():
            host, _, kwargs = hspec
            user = kwargs['user']
            key_file = kwargs['key_file']
            if name not in self:
                specs[name] = callspec(_do_connect, host, user, key_file)
        results, failures = _parallel_do(specs)

        # Pull results into self
        for name, value in results.iteritems():
            host, client, session, top_dir = value
            self[name] = _SSHClientState(name, host, client, session,
                                         top_dir)

        if len(failures) > 0:
            exc = MultiException(
                'Exception(s) raised during call to multiadd():')
            exc.exceptions = failures
            raise exc

    def default_timeout(self, name, timeout):
        """Set the default timeout for an endpoint."""
        self[name].default_timeout = timeout

    def next_timeout(self, name, timeout):
        """Set the timeout for the next call to an endpoint."""
        self[name].next_timeout = timeout

    def get_client(self, host):
        """Get a paramiko.client.SSHClient instance for an existing host."""
        return self._hosts[host]

    def _new_client(self):
        client = paramiko.client.SSHClient()
        if self._hostkey_callback is not None:
            client.set_missing_host_key_policy(
                _SSHHostKeyPolicy(self._hostkey_callback))
        else:
            client.set_missing_host_key_policy(
                paramiko.client.RejectPolicy())
        client.load_system_host_keys()
        client.load_host_keys(os.path.expanduser(self.known_hosts))
        return client

def _session_setup(client):
    top_dir = _create_temp_dir(client)

    # Enable keepalives to appease the firewall gods
    client.get_transport().set_keepalive(10)

    # Discover paths, accounting for both zipapp and bare dev
    # directory.  Try zipapp first
    rpc_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    rpc_path = os.path.join(rpc_dir, ZIPNAME)
    if not os.path.isfile(rpc_path):
        # Looks like we're running in a bare dev directory, so use
        # working directory instead
        rpc_dir = os.path.realpath(os.getcwd())
        rpc_path = os.path.join(rpc_dir, ZIPNAME)

    # Send this script to remote host
    exec_path = os.path.join(top_dir, ZIPNAME)
    with client.open_sftp() as sftp:
        sftp.put(rpc_path, exec_path)
        sftp.chmod(exec_path, 0755)

    # Start up a channel and kick off the session
    session = client.get_transport().open_session()
    session.set_name('python-rpc')
    session.exec_command('%s -r %s' % (exec_path, top_dir))
    return session, top_dir

def _session_cleanup(client, top_dir):
    _remove_temp_dir(client, top_dir)

def _create_temp_dir(client):
    with client.get_transport().open_session() as chan:
        chan.exec_command('mktemp --tmpdir -d %s.XXXXXXXXXX' % (ZIPNAME))
        chan.shutdown_write()
        tmp_dir = chan.recv(10240).strip()
        chan.recv_exit_status()
    return tmp_dir

def _remove_temp_dir(client, top_dir):
    # Clean up temporary directory, with some simple sanity checks
    try:
        with client.open_sftp() as sftp, \
             client.get_transport().open_session() as chan:
            if os.path.realpath(top_dir) == "/":
                raise IOError('BUG: Attempt to recursively remove /, aborting')
            sftp.chdir(top_dir)
            sftp.stat(ZIPNAME)  # Bail if this directory does not have our ZIP
            chan.exec_command('rm -rf %s' % (top_dir))
            chan.recv_exit_status()
    except IOError:
        logger.warning('Unable to clean up endpoint, printing exception and '
                       'resuming cleanup:')
        traceback.print_exc()

def _clean_all_endpoints():
    shutdown()
atexit.register(_clean_all_endpoints)


#
# FILE DESCRIPTOR REDIRECTION
#
# Context manager to redirect file descriptors.
# Inspired by http://stackoverflow.com/a/22434262.
#

# pylint: disable=invalid-name

def _fileno(file_or_fd):
    fd = getattr(file_or_fd, 'fileno', lambda: file_or_fd)()
    if not isinstance(fd, int):
        raise ValueError("Expected a file (`.fileno()`) or a file descriptor")
    return fd

@contextmanager
def redirect(out, to=os.devnull):
    """Redirect one file descriptor to another."""
    out_fd = _fileno(out)
    # copy out_fd before it is overwritten
    #NOTE: `copied` is inheritable on Windows when duplicating a standard stream
    with os.fdopen(os.dup(out_fd), 'wb') as copied:
        out.flush()  # flush library buffers that dup2 knows nothing about
        try:
            os.dup2(_fileno(to), out_fd)  # $ exec >&to
        except ValueError:  # filename
            with open(to, 'wb') as to_file:
                os.dup2(to_file.fileno(), out_fd)  # $ exec > to
        try:
            yield out # allow code to be run with the redirected out
        finally:
            # restore out to its previous value
            #NOTE: dup2 makes out_fd inheritable unconditionally
            out.flush()
            os.dup2(copied.fileno(), out_fd)  # $ exec >&copied


#
# REMOTE EXCEPTION SUPPORT
#
# Replace the standard sys.excepthook with a wrapper that understands
# RPC exceptions.
#
_ORIG_HOOK = sys.excepthook

def _format_rpc_traceback(exception):
    exctype, value, extracted_tb = exception.rpc_traceback
    formatted_tb = ''
    for line in traceback.format_list(extracted_tb):
        formatted_tb += line
    formatted_exc = ''
    for line in traceback.format_exception_only(exctype, value):
        formatted_exc += line

    return '%(formatted_tb)s%(formatted_exc)s' % vars()

def _format_multi_traceback(exctype, value, tback):
    formatted_tb = ''
    for line in traceback.format_exception(exctype, value, tback):
        formatted_tb += line
    return '%(formatted_tb)s' % vars()

def _format_rpc_host(exception):
    host = '(unknown)'
    if 'rpc_host' in exception.__dict__:
        host = exception.rpc_host
    return host

def _maybe_print_rpc_exception(value, pad=''):
    # If exception looks like an RPC traceback, print out the original
    # exception too
    if 'rpc_traceback' in value.__dict__:
        tback = _format_rpc_traceback(value)
        host = _format_rpc_host(value)

        tbackpad = pad + '    '
        msg = pad + '==== Above occurred REMOTELY on endpoint %s, ' \
            'original follows:\n' % (host)
        msg += pad + '    Traceback (most recent call last):\n'
        msg += pad + '    ' + tbackpad.join(tback.splitlines(True))
        msg += pad + '---- End of remote traceback\n'
        sys.stderr.write(msg)

def _maybe_print_multi_exception(outer_value):
    # If exception looks like a multiadd()/multicall() exception,
    # print out all exceptions within
    if 'exceptions' in outer_value.__dict__:
        for name in outer_value.exceptions:
            exctype, value, tback = outer_value.exceptions[name]
            tback = _format_multi_traceback(exctype, value, tback)
            msg = '==== While communicating with endpoint %s:\n' \
                  % (name)
            msg += '    ' + '    '.join(tback.splitlines(True))
            sys.stderr.write(msg)
            _maybe_print_rpc_exception(value, pad='    ')
            msg = "---- End of endpoint %s's traceback\n" % (name)
            sys.stderr.write(msg)

def _rpc_excepthook(exctype, value, tback):
    # Call standard exception hook regardless
    _ORIG_HOOK(exctype, value, tback)
    _maybe_print_rpc_exception(value)
    _maybe_print_multi_exception(value)


def _install_thread_excepthook():
    """Use sys.excepthook for all threads, too.

    Inspired by http://bugs.python.org/issue1230540.

    Call once from __main__ before creating any threads.  If using
    psyco, call psyco.cannotcompile(threading.Thread.run) since this
    replaces a new-style class method.

    """
    init_old = threading.Thread.__init__
    def _init(self, *args, **kwargs):
        init_old(self, *args, **kwargs)
        run_old = self.run
        def _run_with_except_hook(*args, **kw):
            try:
                run_old(*args, **kw)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:  # pylint: disable=bare-except
                sys.excepthook(*sys.exc_info())
        self.run = _run_with_except_hook
    threading.Thread.__init__ = _init

sys.excepthook = _rpc_excepthook
_install_thread_excepthook()
