#!/usr/bin/python
#
# A script capable of compiling and running a SoFASTER server
#    ./server.py -h

import os
import argparse
import subprocess

from datetime import datetime
from psutil import virtual_memory as vm
from multiprocessing import cpu_count as cores

"""
    Returns a configuration dictionary that can be used to run a server.
"""
def defaultCfg():
    serverCfg = {
        "threads"  : "1",
        "bucketM"  : "128",
        "logSzGB"  : "16",
        "logDisk"  : "storage",
        "ipAddr"   : "10.0.0.1",
        "serverId" : "1",
        "dfsConn"  : "=UseDevelopmentStorage=true;",
        "pinOffset": "0"
    }

    return serverCfg

"""
    Generates a server configuration from a set of command line args.
"""
def generateCfg(args):
    cfg = defaultCfg()

    # Calculate the number of cores available to us depending on whether we
    # need softirqs pinned to dedicated cores. Raise an exception if we don't
    # have sufficient cores.
    avail = cores() - 8 if args.pinirq else cores()
    if avail < 0 or (args.threads and args.threads > avail):
        raise Exception("Insufficient cores to run server.")

    # Set threads to the number of cores if we don't have a CLI argument.
    cfg["threads"] = str(args.threads) if args.threads else str(avail)
    cfg["ipAddr"] = args.ip
    cfg["serverId"] = str(args.id)

    if args.buckets: cfg["bucketM"] = str(2**args.buckets)
    if args.logSizeGB: cfg["logSzGB"] = str(args.logSizeGB)
    if args.logDisk: cfg["logDisk"] = args.logDisk
    if args.dfsKey: cfg["dfsConn"] = args.dfsKey
    if args.pinirq: cfg["pinOffset"] = str(8)

    # Do not allow allocating more than 80% of available DRAM. We might need
    # the 20% for overflow buckets.
    m = float(cfg["bucketM"]) * 64 + float(cfg["logSzGB"]) * (1024**3)
    if m > 0.8 * vm().available:
        raise Exception("Insufficient memory to run server.")

    return cfg

"""
    Pins network softirqs to a dedicated set of cores.
"""
def pinirqs():
    p = "cc/scripts/linux/common/softirq.sh"
    c = os.path.join(os.getcwd(), p)
    subprocess.check_call(['sudo', c])

"""
    Runs a server given a binary and configuration.
"""
def run(binary, cfg, debug=False):
    # Create a directory to store the hybrid log.
    try:
        subprocess.check_call(['mkdir', '-p', cfg["logDisk"] + "/hlog/"])
    except:
        print("Failed to create folder/directory to store the hybrid log")
        raise

    # Create a directory to store the logger's output. Link it to a folder
    # called "latest".
    try:
        d = 'logs/server/' + datetime.now().isoformat()
        subprocess.check_call(['mkdir', '-p', d])
        f = os.path.join(os.getcwd(), d)
        subprocess.check_call(['rm', '-Rf', 'logs/server/latest'])
        subprocess.check_call(['ln', '-f', '-s', f, 'logs/server/latest'])
    except:
        print("Failed to create folder/directory to store logger output.")
        raise

    # Launch the server. Make sure we load jemalloc and libraries in
    # /usr/local -- this is where blob store gets installed. Use gdb
    # if we're running in debug mode.
    e = {
        'LD_PRELOAD': '/usr/lib/x86_64-linux-gnu/libjemalloc.so.1',
        'LD_LIBRARY_PATH': '/usr/local/lib',
    }

    l = [binary, "--threads", cfg["threads"], "--htSizeM", cfg["bucketM"],
         "--logSizeGB", cfg["logSzGB"], "--logDisk", cfg["logDisk"], "--ipAddr",
         cfg["ipAddr"], "--dfs", cfg["dfsConn"],
         "--pinOffset", cfg["pinOffset"]]
    if debug: l = ['gdb', '--args'] + l

    # Redirect log messages to a file and also print it to the terminal.
    p = d + '/server' + cfg["serverId"] + '.log'
    s = subprocess.Popen(l, env=e, stderr=subprocess.PIPE)
    t = subprocess.Popen(['tee', p], stdin=s.stderr)
    s.stderr.close()
    t.communicate()

    return

"""
    Creates Makefiles and compiles a server binary.

    @param infrc
            Flag indicating whether we should compile in an Infiniband (True)
            or TCP networking stack at the server's sessions layer.
    @param value
            Size of values in bytes the server must support.
    @param blobStore
            Flag indicating whether we should compile in a remote-blob store
            tier (True) at the persistent log layer.
    @param debug
            Flag indicating whether the binary should have debug symbols.
"""
def compile(infrc=False, value=8, blobStore=False, debug=False):
    # First, setup debug and release build directories and Makefiles.
    try:
        n = 'ON' if infrc == True else 'OFF'
        b = 'ON' if blobStore == True else 'OFF'
        subprocess.check_call(['./cc/scripts/linux/common/sofaster.sh',
                               n, str(value), b])
    except:
        print("Failed to generate Makefiles for compilation")
        raise

    # Next, compile either a debug or release build of the server.
    try:
        build = './cc/build/' + ('Debug' if debug == True else 'Release')
        subprocess.Popen(['make', 'sofaster-server'], cwd=build).wait()
    except:
        print("Failed to compile server binary")
        raise

    return

"""
    Parses command line options and runs a server.
"""
def main():
    parser = argparse.ArgumentParser(description='Compiles or runs a' + \
                                     ' SoFASTER server')

    # This script can be used to either compile or run a server.
    parser.add_argument('mode', type=str, choices=['compile', 'run'],
                        help='Whether the script should compile or run' + \
                        ' the server.')

    # Parameters/Arguments that effect how we run the server binary.
    parser.add_argument('--binary', type=str, required=False,
                        help='Path to the server binary. Ignored if' + \
                        ' using the script to compile.', metavar='path')
    parser.add_argument('--threads', type=int, required=False,
                        help='Number of server threads to run.',
                        metavar='num')
    parser.add_argument('--buckets', type=int, required=False,
                        help='Number of hash bits that the server must' + \
                        ' use to index FASTER\'s hash bucket array.',
                        metavar='bits')
    parser.add_argument('--logSizeGB', type=int, required=False,
                        help='Size of the hybrid log that the server must' + \
                        ' allocate in GB', metavar='GB')
    parser.add_argument('--logDisk', type=str, required=False,
                        help='Folder/Directory name under which the hybrid' + \
                        ' log and session metadata should be stored',
                        metavar='path')
    parser.add_argument('--ip', type=str, required=False,
                        help='IP address that the server must listen to for' + \
                        ' incoming connections.', metavar='address')
    parser.add_argument('--id', type=int, required=False,
                        help='Unique 16-bit server identifier.',
                        metavar='identifier')
    parser.add_argument('--dfsKey', type=str, required=False,
                        help='Connection string for Azure blob store.',
                        metavar='conn str')
    parser.add_argument('--debug', action='store_true',
                        help='Run a debug build of the server.')
    parser.add_argument('--pinirq', action='store_true',
                        help='If true, pins network softirqs to cores 0-7.')

    # Parameters/Arguments that effect how we compile the server binary.
    parser.add_argument('--compile-value', type=int, default=8, metavar='B',
                        help='Value size that the server will be compiled with.')
    parser.add_argument('--compile-infrc', action='store_true',
                        help='Use Infiniband for networking. If absent,' + \
                        ' then use TCP.')
    parser.add_argument('--compile-blobs', action='store_true',
                        help='Use Azure page blobs for the hlog along' + \
                        ' with local SSD.')
    parser.add_argument('--compile-debug', action='store_true',
                        help='If true, compile a Debug build. Otherwise,' + \
                        ' compile a Release build. Ignored if using the' + \
                        ' script to run a server.')
    args = parser.parse_args()

    # Script invoked with mode == compile. Compile server and exit.
    if args.mode == 'compile':
        compile(args.compile_infrc, args.compile_value, args.compile_blobs,
                args.compile_debug)
        return

    # If we're running the server, then make sure we have an IP address.
    if not args.ip:
        parser.error('Cannot run server without an IP address. Option run' + \
                     ' requires option --ip.')

    # If we're running the server, then make sure we have an identifier.
    if not args.id:
        args.id = hash(args.ip) & 65535

    # Figure out which server binary to use; Debug, Release or user provided.
    p = "cc/build/" + ("Debug" if args.debug else "Release") + "/sofaster-server"
    b = os.path.join(os.getcwd(), p)

    if args.binary:
        b = args.binary

    # Launch the server after generating a config from the passed in args and
    # pinning softirqs (if required)
    cfg = generateCfg(args)
    if args.pinirq: pinirqs()

    run(b, cfg, args.debug)

    return

if __name__ == "__main__":
    main()
