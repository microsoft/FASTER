#!/usr/bin/python
#
# Benchmarks a SoFASTER server's thread scalability.

import os
import numpy
import argparse
import subprocess

from multiprocessing import cpu_count as cores

"""
    Colors to distinguish between runs.
"""
class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

"""
    Basic config info required to run the experiment.
"""
defaultCfg = {
    "server": "192.168.0.1",
    "lFile":  "ycsb.load.250000000",
    "tFile":  "ycsb.txns.250000000.1000000000",
}
"""
    Given a config, loads YCSB data into the server from workload files.
"""
def load(cfg):
    subprocess.check_call(['./scripts/common/ycsb.py', 'run', '--no-ycsb',
                           '--servers', cfg["server"],
                           '--loadFile', cfg["lFile"],
                           '--txnsFile', cfg["tFile"]])

"""
    Given a config and number of threads, performs one run of the experiment.
"""
def expt(cfg, threads):
    subprocess.check_call(['./scripts/common/ycsb.py', 'run', '--no-fill',
                           '--servers', cfg["server"],
                           '--loadFile', cfg["lFile"],
                           '--txnsFile', cfg["tFile"],
                           '--duration', "120",
                           '--threads', str(threads)])

"""
    Parses command line arguments and runs the experiment.
"""
def main():
    parser = argparse.ArgumentParser(description='Runs a thread scalability' + \
                                     ' experiment against a SoFASTER server.')

    # This script requires three arguments to run the experiment.
    parser.add_argument('--server', type=str, required=True, metavar='IP',
                        help='IP address of server the experiment must' + \
                        ' be run against. Ex: \"10.0.0.1\".')
    parser.add_argument('--loadFile', type=str, required=True, metavar='path',
                        help='File containing keys to be loaded into server.')
    parser.add_argument('--txnsFile', type=str, required=True, metavar='path',
                        help='File with txns to be executed against server.')
    args = parser.parse_args()

    # Create a config that can be used to load the server and run the expt.
    cfg = defaultCfg
    cfg["server"] = args.server
    cfg["lFile"] = args.loadFile
    cfg["tFile"] = args.txnsFile

    # Load the server once. Experimental runs will reuse the same instance.
    print color.RED + "\n[Loading Data Into Server]\n" + color.END
    load(cfg)

    # Vary the number of threads issuing a YCSB load to the server.
    runs = numpy.random.permutation(range(0, cores() + 1, 4))
    for t in runs:
        print color.RED + "\n[Running Experiment With " + str(t) + \
              " Threads]\n" + color.END
        expt(cfg, 1 if t == 0 else t)

if __name__ == "__main__":
    main()
