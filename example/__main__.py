"""Example for rpc.py."""

import sys
import optparse
import logging
import socket
import rpc

#logging.raiseExceptions = False
logging.basicConfig(level=logging.WARNING)

def main():
    """Main entry point to script."""

    # Tell rpc module our name
    rpc.ZIPNAME = 'example'

    # Process command line options
    usage = 'Usage: %prog HOST'
    description = """\
    rpc.py Example
    """
    parser = optparse.OptionParser(usage=usage, description=description)
    parser.add_option(
        '-r', '--rpc', dest='rpc', action='store_true', default=False,
        help='RPC mode: expect serialized functions on stdin')

    options, args = parser.parse_args()

    # Redirect to RPC mode if requested -- this is how the script will
    # be invoked remotely, so process it before worrying about other
    # arguments, positional parameters, etc.
    if options.rpc:
        rpc.server_loop()
        sys.exit(0)

    if len(args) != 1:
        parser.print_usage()
        sys.exit(1)
    host = args[0]

    # Connect to the specified host
    rpc.add('hello_host', host)

    # The function that we will run remotely
    def hello_world():
        # Uncomment this and see what happens!
        #1 / 0

        # Return the hostname so we have proof of remote execution
        return socket.gethostname()

    # Now call it and print the results
    result = rpc.call('hello_host', hello_world)
    print "Received `%s' from host %s" % (result, host)

try:
    main()
except KeyboardInterrupt:
    pass
