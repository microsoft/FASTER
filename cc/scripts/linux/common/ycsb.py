#!/usr/bin/python
#
# A script capable of compiling and running a YCSB client for SoFASTER
#    ./ycsb.py -h

import os
import argparse
import subprocess

from datetime import datetime
from multiprocessing import cpu_count as cores

"""
    Returns a configuration dictionary that can be used to run a YCSB client.
"""
def defaultCfg():
    clientCfg = {
        "type"     : "YCSB-F",
        "threads"  : "1",
        "nKeys"    : "250000000",
        "nTxns"    : "1000000000",
        "lFile"    : "ycsb.load.250000000",
        "tFile"    : "ycsb.txns.250000000.1000000000",
        "servers"  : ["192.168.0.1"],
        "duration" : "360",
        "sample"   : "false",
        "fill"     : "true",
        "no-ycsb"  : "false",
        "pinOffset": "0",
        "generateK": "true"
    }

    return clientCfg

"""
    Generates a client configuration from a set of command line args.
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
    cfg["servers"] = args.servers.split(',')

    if args.type: cfg["type"] = args.type
    if args.nKeys: cfg["nKeys"] = str(args.nKeys)
    if args.nTxns: cfg["nTxns"] = str(args.nTxns)
    if args.duration: cfg["duration"] = str(args.duration)
    if args.sample: cfg["sample"] = "true"
    if args.no_fill: cfg["fill"] = "false"
    if args.no_ycsb: cfg["no-ycsb"] = "true"
    if args.pinirq: cfg["pinOffset"] = str(8)

    # If we're generating for workload files, then set the config appropriately.
    if args.loadFile:
        cfg["lFile"] = args.loadFile
        cfg["tFile"] = args.txnsFile
        cfg["generateK"] = "false"

    return cfg

"""
    Pins network softirqs to a dedicated set of cores.
"""
def pinirqs():
    p = "cc/scripts/linux/common/softirq.sh"
    c = os.path.join(os.getcwd(), p)
    subprocess.check_call(['sudo', c])

"""
    Runs a client given a binary and configuration.
"""
def run(binary, cfg, debug=False):
    # Create a directory to store the logger's output. Link it to a folder
    # called "latest".
    try:
        d = 'logs/client/' + datetime.now().isoformat()
        subprocess.check_call(['mkdir', '-p', d])
        f = os.path.join(os.getcwd(), d)
        subprocess.check_call(['rm', '-Rf', 'logs/client/latest'])
        subprocess.check_call(['ln', '-f', '-s', f, 'logs/client/latest'])
    except:
        print("Failed to create folder/directory to store logger output.")
        raise

    # Launch the client. Make sure we load in jemalloc.
    e = {
        'LD_PRELOAD': '/usr/lib/x86_64-linux-gnu/libjemalloc.so.1',
    }

    l = [binary, "--threads", cfg["threads"], "--nKeys", cfg["nKeys"], "--nTxns",
         cfg["nTxns"], "--loadFile", cfg["lFile"], "--txnsFile", cfg["tFile"],
         "--exptTimeSec", cfg["duration"], "--sample", cfg["sample"],
         "--fillAndExit", cfg["no-ycsb"], "--fillServers", cfg["fill"],
         "--pinOffset", cfg["pinOffset"], "--generateKeys", cfg["generateK"],
         "--workload", cfg["type"]]
    l = l + ["--servers"] + cfg["servers"]
    if debug: l = ['gdb', '--args'] + l

    # Redirect log messages to a file and also print it to the terminal.
    p = d + '/ycsb.log'
    s = subprocess.Popen(l, env=e, stderr=subprocess.PIPE)
    t = subprocess.Popen(['tee', p], stdin=s.stderr)
    s.stderr.close()
    t.communicate()

    return

"""
    Creates Makefiles and compiles a client binary.

    @param infrc
            Flag indicating whether we should compile in an Infiniband (True)
            or TCP networking stack at the client's sessions layer.
    @param value
            Size of values in bytes the client must support.
    @param debug
            Flag indicating whether the binary should have debug symbols.
"""
def compile(infrc=False, value=8, debug=False):
    # First, setup debug and release build directories and Makefiles.
    try:
        n = 'ON' if infrc == True else 'OFF'
        subprocess.check_call(['./cc/scripts/linux/common/sofaster.sh',
                               n, str(value)])
    except:
        print("Failed to generate Makefiles for compilation")
        raise

    # Next, compile either a debug or release build of the YCSB client.
    try:
        build = './cc/build/' + ('Debug' if debug == True else 'Release')
        subprocess.Popen(['make', 'sofaster-client'], cwd=build).wait()
    except:
        print("Failed to compile client binary")
        raise

    return

"""
    Parses command line options and runs a YCSB client.
"""
def main():
    parser = argparse.ArgumentParser(description='Compiles or runs a' + \
                                     ' YCSB client for SoFASTER.')

    # This script can be used to either compile or run the client.
    parser.add_argument('mode', type=str, choices=['compile', 'run'],
                        help='Whether the script should compile or run' + \
                        ' the client.')

    # Parameters/Arguments that effect how we run the client binary.
    parser.add_argument('--servers', type=str, required=False, metavar='IPs',
                        help='Comma separated list of server IP addresses' + \
                        ' to run YCSB against. Ex: \"10.0.0.1,10.0.0.2\".')
    parser.add_argument('--binary', type=str, required=False,
                        help='Path to the client binary. Ignored if' + \
                        ' using the script to compile.', metavar='path')
    parser.add_argument('--type', type=str, required=False, metavar='workload',
                        choices=['YCSB-A', 'YCSB-B', 'YCSB-C', 'YCSB-D',
                        'YCSB-F'], help='Type of YCSB workload to run.' + \
                        ' Supported types are YCSB-A, YCSB-B, YCSB-C,' + \
                        ' YCSB-D, YCSB-F')
    parser.add_argument('--threads', type=int, required=False,
                        help='Number of client threads to run.',
                        metavar='num')
    parser.add_argument('--nKeys', type=int, required=False, metavar='num',
                        help='Number of keys contained within the database.')
    parser.add_argument('--nTxns', type=int, required=False, metavar='num',
                        help='Number of YCSB transactions to loop over.')
    parser.add_argument('--loadFile', type=str, required=False, metavar='path',
                        help='File containing keys to be loaded into SoFASTER.')
    parser.add_argument('--txnsFile', type=str, required=False, metavar='path',
                        help='File with txns to be executed against SoFASTER.')
    parser.add_argument('--duration', type=int, required=False, metavar='secs',
                        help='Duration (secs) for which to run YCSB.')
    parser.add_argument('--sample', action='store_true',
                        help='Periodically sample throughput at the client.')
    parser.add_argument('--no-fill', action='store_true',
                        help='Run YCSB without filling data into SoFASTER.')
    parser.add_argument('--no-ycsb', action='store_true',
                        help='Fill data into SoFASTER but don\'t run YCSB.')
    parser.add_argument('--debug', action='store_true',
                        help='Run a debug build of the client.')
    parser.add_argument('--pinirq', action='store_true',
                        help='If true, pins network softirqs to cores 0-7.')

    # Parameters/Arguments that effect how we compile the client binary.
    parser.add_argument('--compile-value', type=int, default=8, metavar='B',
                        help='Value size that servers were compiled with.')
    parser.add_argument('--compile-infrc', action='store_true',
                        help='Use Infiniband for networking. If absent,' + \
                        ' then use TCP.')
    parser.add_argument('--compile-debug', action='store_true',
                        help='If true, compile a Debug build. Otherwise,' + \
                        ' compile a Release build. Ignored if using the' + \
                        ' script to run a client.')
    args = parser.parse_args()

    # Script invoked with mode == compile. Compile client and exit.
    if args.mode == 'compile':
        compile(args.compile_infrc, args.compile_value, args.compile_debug)
        return

    # If we're running the client, make sure we have servers to run against.
    if not args.servers:
        parser.error('Cannot run YCSB without a comma separated list' + \
                     ' of server IP addresses.')

    # If we've got a load file, make sure we have a txns file too.
    if args.loadFile and not args.txnsFile:
        parser.error('Cannot run YCSB with a load file but no txns file.')

    # Figure out which client binary to use; Debug, Release or user provided.
    p = "cc/build/" + ("Debug" if args.debug else "Release") + "/sofaster-client"
    b = os.path.join(os.getcwd(), p)

    if args.binary:
        b = args.binary

    # Launch the client after generating a config from the passed in args and
    # pinning softirqs (if required)
    cfg = generateCfg(args)
    if args.pinirq: pinirqs()

    run(b, cfg, args.debug)

if __name__ == "__main__":
    main()
