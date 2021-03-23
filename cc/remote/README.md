# Distributed FASTER

This repo contains scripts to automate setup, compilation and running of
servers and clients on Azure VMs running Ubuntu and on [CloudLab](https://www.cloudlab.us).

On Azure, these scripts have been tested on the
[D48v3](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/sizes-general)
and [HB60rs](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/sizes-hpc)
class of VMs running Ubuntu server 18.04 but should work fine on others too.

On CloudLab, these scripts have been tested on the
[c6420 and c6220](https://docs.cloudlab.us/hardware.html) class of machines running
Ubuntu server 16.04 but should work fine on others too. A sample CloudLab profile is
available [here](https://www.cloudlab.us/p/sandstorm/sofaster).

By default, the scripts configure and run a TCP version of the system. To setup an Azure
VM for the Infiniband version, follow the instructions
[here](#setting-up-the-infiniband-version-on-azure). To setup a CloudLab machine for the
Infiniband version, follow the instructions
[here](#setting-up-the-infiniband-version-on-cloudlab).

### Installing dependencies

Before the server and client binaries can be compiled and run, few dependencies need
to be installed. Doing so requires root permissions. Run the following from the
project's root folder
```
~/SoFASTER$ sudo ./scripts/common/deps.sh
```

### Compiling a server

To compile the server, run the following script from the project's root folder
```
server:~/SoFASTER$ ./scripts/common/server.py compile
```
The above command will setup `Debug` and `Release` Makefiles and directories, and
compile a `Release` build of the server, stored under `cc/build/Release`. To compile
a `Debug` build (stored under `cc/build/Debug`), run the above script with the
`--compile-debug` flag as follows
```
server:~/SoFASTER$ ./scripts/common/server.py compile --compile-debug
```

### Running a server

Server's take in two compulsory arguments: An IP address to listen on for incoming
client connections and a 16-bit identifier. On Linux, the set of active network
interfaces and their IP addresses can be obtained by running `ifconfig`
```
server:~/SoFASTER$ ifconfig
eth0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 10.0.0.30  netmask 255.255.255.0  broadcast 10.0.0.255
        inet6 fe80::20d:3aff:fe5e:a54  prefixlen 64  scopeid 0x20<link>
        ether 00:0d:3a:5e:0a:54  txqueuelen 1000  (Ethernet)
        RX packets 105476818  bytes 1670249987005 (1.6 TB)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 104972812  bytes 919740800326 (919.7 GB)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0
```
For best performance, it is recommended to chose a high speed (> 10Gbps) network
interface for the server. Speeds can be found using the `ethtool` program on Linux
```
server:~/SoFASTER$ ethtool eth0 | grep Speed
	Speed: 40000Mb/s
```
To run a server, invoke the same script that was used for compilation with `run`
along with an IP address and identifier as arguments (do so from the project's
root folder).
```
server:~/SoFASTER$ ./scripts/common/server.py run --ip 10.0.0.30 --id 1
[1580838640.253452741]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-server.cc:main:117:: Running server with 48 worker threads, 128 M hash buckets, 16 GB hybrid log stored at storage, with a mutable fraction of 0.90, and sampled set of 128 KB
```
By default, the server runs a thread per core and allocates a hash table with 128
million buckets, and a hybrid log of size 16GB. These and other variables can be
changed by passing in additional arguments to the script. Invoke the script with
`-h` for a full list.
```
server:~/SoFASTER$ ./scripts/common/server.py -h
```

### Compiling a YCSB client

To compile a YCSB client, invoke the following script from the project's root
folder
```
client:~/SoFASTER$ ./scripts/common/ycsb.py compile
```
This script is similar to the one for compiling a server; it setups up Makefiles
and directories for `Debug` and `Release` builds, and compiles a `Release` build.
To compile a `Debug` build, add a `--compile-debug` flag as follows
```
client:~/SoFASTER$ ./scripts/common/ycsb.py compile --compile-debug
```

### Running a YCSB client against a server

The YCSB client takes in one compulsory argument: a comma separated list of servers.
It splits a 64 bit hash space evenly across these servers and issues the workload
against them. To run a client, invoke the same script that was used for compilation
with `run` and a list of server IP addresses as arguments from the project's root
folder. The example below uses only one server.
```
client:~/SoFASTER$ ./scripts/common/ycsb.py run --servers 10.0.0.30
[1580838675.301505995]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:433:: Running client with 48 threads. Each thread will run YCSB-F with 250000000 keys and 1000000000 transactions for 360 seconds. Requests will be issued in batches of 32768 B with a pipeline of size 2
[1580838675.301529195]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:440:: Generating workload keys and requests from PRNG
[1580838675.301531895]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:444:: Filling data into the server
[1580838687.441638608]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:463:: Running YCSB-F benchmark
[1580839047.492461494]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:517:: Completed Experiment
[1580839047.492550195]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:524:: Average Throughput: 75.673 Mops/sec
[1580839047.492586596]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:540:: Waiting for threads to exit
[1580839047.581496479]::INFO::/home/chinmayk/SoFASTER/cc/src/client/sofaster.h:~Sofaster:126:: Median Latency: 2143.01 microseconds
```
By default, the client runs one thread per core and issues a billion requests against
250 million records for 60 seconds. Requests are chosen uniformly at random. These and
other variables can be changed by passing in additional arguments to the script. Invoke
the script with `-h` for a full list.
```
client:~/SoFASTER$ ./scripts/common/ycsb.py -h
```

### Using YCSB workload files

The YCSB client also supports running a pre-generated workload. To run in this mode,
first follow the instructions at the [YCSB repo](https://github.com/brianfrankcooper/YCSB)
to generate `load` and `run` datasets using the `basic` interface for the workload of your
choice (we currently support A, B, C, D, F).

Next, these datasets need to be processed into a format that our client can read. To do
so, this repo contains a program called `process_ycsb` under `cc/benchmark-dir`
```
client:~/SoFASTER/cc/benchmark-dir$ g++ -o process_ycsb process_ycsb.cc -lboost_program_options
client:~/SoFASTER/cc/benchmark-dir$ ./process_ycsb --from raw_load_file --dest ~/SoFASTER/ycsb.load
client:~/SoFASTER/cc/benchmark-dir$ ./process_ycsb --from raw_run_file --dest ~/SoFASTER/ycsb.txns
```

To run the client using the above workload files (assuming we have 250 million records in
the load file and 1 billion zipfian skewed requests in the run file), invoke the client
script from the project's root folder as follows
```
client:~/SoFASTER$ ./scripts/common/ycsb.py run --servers 10.0.0.30 --nKeys 250000000 --loadFile ~/SoFASTER/ycsb.load --nTxns 1000000000 --txnsFile ~/SoFASTER/ycsb.txns
[1580839988.997521074]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:433:: Running client with 48 threads. Each thread will run YCSB-F with 250000000 keys and 1000000000 transactions for 360 seconds. Requests will be issued in batches of 32768 B with a pipeline of size 2
[1580839988.997543575]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:437:: Loading workload data from /mnt/ycsb.load.250000000 and /mnt/ycsb.txns.250000000.1000000000 into memory
[1580840033.577002385]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:444:: Filling data into the server
[1580840046.368219223]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:463:: Running YCSB-F benchmark
[1580840406.425430372]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:517:: Completed Experiment
[1580840406.425694574]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:524:: Average Throughput: 86.038 Mops/sec
[1580840406.425708474]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:540:: Waiting for threads to exit
[1580840406.517582989]::INFO::/home/chinmayk/SoFASTER/cc/src/client/sofaster.h:~Sofaster:126:: Median Latency: 1899.72 microseconds
```

### Accessing server and client logs
All messages printed out by the server and client at runtime are also written to log
files stored under `logs/server/` and `logs/client/`. A special folder called `latest`
links to the most recent log file.
```
server:~/SoFASTER$ cat logs/server/latest/server1.log 
[1580842843.848360327]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-server.cc:main:117:: Running server with 40 worker threads, 128 M hash buckets, 128 GB hybrid log stored at storage, with a mutable fraction of 0.90, and sampled set of 128 KB
```
```
client:~/SoFASTER$ cat logs/client/latest/ycsb.log
[1580840470.213499543]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:433:: Running client with 40 threads. Each thread will run YCSB-F with 250000000 keys and 1000000000 transactions for 360 seconds. Requests will be issued in batches of 32768 B with a pipeline of size 2
[1580840470.213522443]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:437:: Loading workload data from /mnt/ycsb.load.250000000 and /mnt/ycsb.txns.250000000.1000000000 into memory
[1580840515.872147306]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:444:: Filling data into the server
[1580840529.351481034]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:463:: Running YCSB-F benchmark
[1580840889.386677848]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:517:: Completed Experiment
[1580840889.386731948]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:524:: Average Throughput: 89.422 Mops/sec
[1580840889.386741648]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:540:: Waiting for threads to exit
[1580840889.458787099]::INFO::/home/chinmayk/SoFASTER/cc/src/client/sofaster.h:~Sofaster:126:: Median Latency: 1662.11 microseconds
```
### Pinning TCP SoftIRQs

Linux's TCP stack is interrupt driven. When data arrives at the network, softirq's
take care of processing and handing off this data to applications. Dedicating a
few cores on the server and client to softirq's can help improve throughput and
latency. Both server and client scripts support a `--pinirq` flag that dedicates
8 cores to network softirqs. Root permissions are required when using this flag.
```
server:~/SoFASTER$ sudo ./scripts/common/server.py run --ip 10.0.0.30 --id 1 --pinirq
[1580842843.848360327]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-server.cc:main:117:: Running server with 40 worker threads, 128 M hash buckets, 128 GB hybrid log stored at storage, with a mutable fraction of 0.90, and sampled set of 128 KB
```
```
client:~/SoFASTER$ sudo ./scripts/common/ycsb.py run --servers 10.0.0.30 --nKeys 250000000 --loadFile ~/SoFASTER/ycsb.load --nTxns 1000000000 --txnsFile ~/SoFASTER/ycsb.txns --pinirq
[1580840470.213499543]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:433:: Running client with 40 threads. Each thread will run YCSB-F with 250000000 keys and 1000000000 transactions for 360 seconds. Requests will be issued in batches of 32768 B with a pipeline of size 2
[1580840470.213522443]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:437:: Loading workload data from /mnt/ycsb.load.250000000 and /mnt/ycsb.txns.250000000.1000000000 into memory
[1580840515.872147306]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:444:: Filling data into the server
[1580840529.351481034]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:463:: Running YCSB-F benchmark
[1580840889.386677848]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:517:: Completed Experiment
[1580840889.386731948]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:524:: Average Throughput: 89.422 Mops/sec
[1580840889.386741648]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:540:: Waiting for threads to exit
[1580840889.458787099]::INFO::/home/chinmayk/SoFASTER/cc/src/client/sofaster.h:~Sofaster:126:: Median Latency: 1662.11 microseconds
```

### Setting up the Infiniband version on Azure

`NOTE: Azure currently requires that VMs be part of the same availability set to be
able to communicate with each other over Infiniband`

This repo contains a script to download and install Mellanox infiniband drivers on
Azure HPC VMs running Ubuntu server 18.04. Running this script requires root
permissions. First, install dependencies required by SoFASTER (do so from the root
folder of the project)
```
~/SoFASTER$ sudo ./scripts/common/deps.sh
```
Next, install and setup inifiniband drivers on the VM (again from the root folder
of the project)
```
~/SoFASTER$ sudo ./scripts/azure/mlnx.sh
```
In addition to installing Mellanox drivers, the above script sets up `IPoIB` on
the VM, allowing servers and clients to connect over infiniband using regular
IP addresses. These IP addresses come from an internal Azure virtual address space
and require the VM to be rebooted after the script completes
```
~/SoFASTER$ sudo reboot
```

### Compiling the Infiniband version on Azure

The infiniband version of the server and client can be compiled by passing in a
`--compile-infrc` flag to the scripts used to compile the TCP versions (again,
to be invoked from the project's root folder)
```
server:~/SoFASTER$ ./scripts/common/server.py compile --compile-infrc
```
```
client:~/SoFASTER$ ./scripts/common/ycsb.py compile --compile-infrc
```

### Running the Infiniband version on Azure

The infiniband version of the client and server can be run using the
[scripts](#running-a-server) for the TCP version -- the arguments
accepted and script behavior are the same for both versions. When
passing in an IP address for the server to listen on, use the address
of the network interface called `ib0`; this is the infinband
interface exposed to the VM.
```
server:~/SoFASTER$ ifconfig
ib0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 2044
        inet 172.16.1.32  netmask 255.255.0.0  broadcast 172.16.255.255
        inet6 fe80::215:5dff:fd33:ff29  prefixlen 64  scopeid 0x20<link>
        unspec 20-00-08-A7-FE-80-00-00-00-00-00-00-00-00-00-00  txqueuelen 256  (UNSPEC)
        RX packets 0  bytes 0 (0.0 B)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 13  bytes 1004 (1.0 KB)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0
```
```
server:~/SoFASTER$ ./scripts/common/server.py run --ip 172.16.1.32 --id 1
[1580922131.079572680]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-server.cc:main:117:: Running server with 60 worker threads, 128 M hash buckets, 16 GB hybrid log stored at storage, with a mutable fraction of 0.90, and sampled set of 128 KB
```
```
client:~/SoFASTER$ ./scripts/common/ycsb.py run --servers 172.16.1.32
[1580922826.317821031]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:433:: Running client with 60 threads. Each thread will run YCSB-F with 250000000 keys and 1000000000 transactions for 360 seconds. Requests will be issued in batches of 4096 B with a pipeline of size 2
[1580922826.317848132]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:440:: Generating workload keys and requests from PRNG
[1580922826.317850232]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:444:: Filling data into the server
[1580922855.234707155]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:463:: Running YCSB-F benchmark
[1580923215.292713702]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:517:: Completed Experiment
[1580923215.292780904]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:524:: Average Throughput: 102.547 Mops/sec
[1580923215.292790904]::INFO::/home/chinmayk/SoFASTER/cc/benchmark-dir/sofaster-client.cc:main:540:: Waiting for threads to exit
[1580923219.875275799]::INFO::/home/chinmayk/SoFASTER/cc/src/client/sofaster.h:~Sofaster:126:: Median Latency: 281.61 microseconds
```

### Setting up the Infiniband version on CloudLab

This repo contains a script to download and install Mellanox infiniband drivers on
a CloudLab machine instantiated using the
[sample profile](https://www.cloudlab.us/p/sandstorm/sofaster). Running this script
requires root permissions. First, install dependencies required by SoFASTER (do so
from the root folder of the project)
```
~/SoFASTER$ sudo ./scripts/common/deps.sh
```
Next, install and setup inifiniband drivers on the machine (again from the
root folder of the project)
```
~/SoFASTER$ sudo ./scripts/cloudLab/mlnx.sh
```
In addition to the drivers, this script also brings up an IPoIB interface called
`ib0` that a server can listen on for incoming connections.

Compiling and running servers and YCSB clients is similar to doing so on Azure. Just
follow the instructions [here](#compiling-the-infiniband-version-on-azure) and
[here](#running-the-infiniband-version-on-azure).
