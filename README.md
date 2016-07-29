rpc.py - Start remote Python interpreters and call functions
============================================================

rpc.py is a tool that uses SSH to start remote Python interpreters and
call functions remotely.  The code should be considered alpha since it
lacks some features, but it has already proven useful to us.


How does it work?
-----------------

rpc.py connects to one or more endpoints over SSH and copies itself to
the remote system.  It then allows you to call functions remotely from
the controlling script.  It uses [Dill][1] for serialization of
functions and data, and [Paramiko][2] for SSH connectivity.

rpc.py will proxy remote exceptions back to the controlling script.

The design currently mandates that rpc.py be packaged into a Python
zipapp that includes itself and the Python code to be executed
remotely.  The resulting zipapp should support a `-r` option that
invokes `rpc.server_loop()`.  See `example/__main__.py` for a
demonstration.

See `example/Makefile` for the zipapp creation procedure.

[1]: https://github.com/uqfoundation/dill  "Dill Repository"
[2]: http://www.paramiko.org/              "Paramiko"


Dependencies
------------

- rpc.py currently requires Python 2.7.  I hope to port it to 3.x in
  the future.  It may be straightforward; I haven't even tried yet!
  Contributions are welcome.

- The example expects the PyCrypto library to be installed on the
  system.  Other dependencies (Dill, Paramiko, and ecdsa) are
  downloaded and packaged into the example zipapp.


Example
-------

An example script using rpc.py is included:

```sh
cd example
make
./example SOME_HOST
```

The example expects that you have passwordless SSH logins configured
for `SOME_HOST`.  It should print out the hostname of the remote system.

For fun, uncomment the `1 / 0` in `example/__main__.py` (in the
`hello_world()` function) to demonstrate exception forwarding.


Contributing
------------

Coming soon!


License
-------

This work is released under the Mozilla Public License 2.0.  See
[LICENSE](LICENSE) at the root of this repository.
