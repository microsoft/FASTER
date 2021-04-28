---
title: "Remote FASTER - Basics"
permalink: /docs/remote-basics/
excerpt: "Remote FASTER"
last_modified_at: 2021-04-12
toc: false
classes: wide
---

FASTER can now be used with remote clients. The basic approach is as follows:

## Creating FasterKV server

You can create and deploy a server based on either fixed-size (blittable struct) Key and Value types or variable-size 
(varlen) Key and Value types similar to byte arrays. You can customize the Input and Output as well as other FASTER settings such
as the hash table size, log size, etc. before deploying the server. `FASTER.server` is the library used to 
create a customized server, and takes an instantiated FASTER store as argument during its creation. We 
provide two example servers out of the box. The server is deployed to listen on a specific IP address and 
port. The default FASTER port is 3278. Right now, the server communicates using a binary protocol, but we
also plan to add support for the RESP protocol so that RESP clients (Redis) in any language can communicate
with FASTER servers.


## Using FasterKV client in remote application

Use `FASTER.client` as a client library in your C# application to connect to any server and run operations remotely. You
can install the client library via NuGet at [Microsoft.FASTER.Client](https://www.nuget.org/packages/Microsoft.FASTER.Client/). The 
library exposes the ability to create a session, very similar to `ClientSession` in embedded deployments. A session targets
a particular IP address and port; we do not provide automatic sharding out of the box right now. Each client session 
establishes a long-running TCP connection to the server. A shadow session is established with FASTER on the server 
side, for each client session. As in local FASTER, you can create multiple sessions to the server in order to benefit 
from server-side parallelism. The session API is async, as remote operations usually take longer to complete. The 
C# client uses a custom binary protocol based on binary serialization of types and bytes. A client defined using 
fixed (or variable) length types should communicate with a server deployed identically.


## Fixed-Length Server and Client

Let us start with a simple case where we want to deploy a client-server that operates over fixed-length 8-byte keys and 8-byes values.

### Server Code

The server wrapper code for this scenario is available at 
[cs/remote/samples/FixedLenServer](https://github.com/microsoft/FASTER/tree/master/cs/remote/samples/FixedLenServer). You can either
clone from GitHub, or create a stand alone program with this code using the server library via NuGet at 
[Microsoft.FASTER.Server](https://www.nuget.org/packages/Microsoft.FASTER.Server/).

The first step is to create a FasterKV instance, recovering from a checkpoint if needed:

```cs
var store = new FasterKV<Key, Value>(indexSize, logSettings, checkpointSettings);
```

Next, we construct the FasterKV server as follows, specifying the IP address to bind, and the port number to listen on. Types 
such as `Key`, `Value`, and `Input` are the fixed-length types this server supports, while `Functions` is the `IFunctions` 
implementation we use.

```cs
var server = new FasterKVServer<Key, Value, Input, Output, Functions, BlittableParameterSerializer<Key, Value, Input, Output>>
   (store, e => new Functions(), opts.Address, opts.Port);
```

In our sample `IFunctions`, RMW is pre-defined to add `Input` to `Value`, so the server behaves as a per-key 
sum computation server, in addition to standard reads and upserts. You can pre-define arbitrarily complex computations based
on `Input`, which can for example, have an enum operation ID inside `Input` for the client to indicate which operation the 
server should do during RMW, on `Value`:

```cs
void InitialUpdater(ref Key key, ref Input input, ref Value value) => value.value = input.value;
bool InPlaceUpdater(ref Key key, ref Input input, ref Value value)
{
   Interlocked.Add(ref value.value, input.value);
   return true;
}
public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue) 
   => newValue.value = input.value + oldValue.value;
```

Finally, we start the server and wait forever, letting the server listen to the specified port and service incoming requests:

```cs
server.Start();
Thread.Sleep(Timeout.Infinite);
```

`FixedLenServer` accepts several command-line arguments such as IP address, port, memory size used by the hash table and 
log, checkpoint folders, etc. For example, you can run the server from the command line to listen on IP address 127.0.0.1 
and port 3278 as follows:

```
> FixedLenServer.exe --bind 127.0.0.1 --port 3278

FASTER fixed-length (binary) KV server
Using page size of 32m
Using log memory size of 16g
There are 512 log pages in memory
Using disk segment size of 1g
Using hash index size of 64m (1m cache lines)
```

The default configuration parameters are shown above; these may be overridden via the command line (use `--help` to see options).

### Client Code

A sample client application that talks to the above server is available at 
[cs/remote/samples/FixedLenClient](https://github.com/microsoft/FASTER/tree/master/cs/remote/samples/FixedLenClient). You
can also install the client library for your application via NuGet at 
[Microsoft.FASTER.Client](https://www.nuget.org/packages/Microsoft.FASTER.Client/).


The client starts simply by defining the client instance that specifies the server IP address and port to connect to:

```cs
using var client = new FasterKVClient<long, long>(ip, port);
```

As you can see above, our client uses `long` keys and values. This is fine to use instead of `Key` and `Value` 
as `long` is binary compatible with those types that were used on the server side. The next step is to instantiate
one or more remote client sessions:

```cs
using var session = client.NewSession(new Functions());
```

Here, `Functions` implement an interface called `ICallbackFunctions`, and provide completion callbacks that 
the client will invoke as operations complete. As with embedded FASTER, a session is mono-threaded, i.e., it is
a sequence of operations invoked serially. You can invoke a batch of operations and wait for their completion. For 
invocation parallelism, one can easily create multiple sessions to the same server. We provide a synchronous
and an asynchronous client API.


#### Sync Client API

In `SyncSamples` we use a synchronous API to communicate with the server. For example, we perform `Upsert` as follows:

```cs
for (int i = 0; i < 1000; i++)
   session.Upsert(i, i + 10000);
```

A synchronous Upsert does not automatically flush batches of data to the network. We can flush as follows:

```cs
session.Flush();
```

As mentioned earlier, in the sync API, clients receive responses via function callbacks specified when creating 
the remote client session. We can perform a read operation and await all previous operations - including the Read - to 
complete as follows (you will receive the read result via the callback):

```cs
session.Read(key: 23);
session.CompletePending(true);
```

`CompletePending` ensures that all previously issued operations are completed (and user callbacks received). Below we 
show two RMW operations and a Read to verify that the RMW operations succeeded:

```cs
session.RMW(23, 25);
session.RMW(23, 25);
session.Read(key: 23);
session.CompletePending(true);
```

The final read will produce a result that is `25+25` more than the older value of `10023`, for key `23`.


#### Async Client API

In `AsyncSamples` we use an asynchronous client API to communicate with the server. For example, we perform an 
async read as follows:

```cs
var (status, output) = await session.ReadAsync(23);
if (status != Status.OK || output != 10023)
   throw new Exception("Error!");
```

By default, the async call flushes the outgoing network buffer immediately during the operation issue. To instead 
batch and flush manually, set `forceFlush = false` in the async calls, and call `Flush` on the session when 
you are ready to flush (e.g., periodically on a separate thread).

### FASTER YCSB benchmark

Our YCSB benchmark uses 8-byte keys and values, and is therefore binary-compatible with `FixedLenServer`. You can 
therefore instantiate a `FixedLenServer` and run the YCSB benchmark to communicate with the server. The benchmark
code is available at 
[cs/remote/benchmark/FASTER.benchmark](https://github.com/microsoft/FASTER/tree/master/cs/remote/benchmark/FASTER.benchmark).
The benchmark takes several command-line parameters, similar to our embedded FasterKV benchmark, but including extra
necessary information such as server IP address and port. 

Make sure `FixedLenServer` is first listening on the same address and port, and has been
instantiated with a sufficiently large hash table (8GB):

```
FixedLenServer -i 8g
```

Below is a sample benchmark run that runs the benchark with 8 sessions (`-t 8`), 50% reads (`-r 50`), Zipf 
distribution (`-d zipf`), connecting to IP address 127.0.0.1 on port 3278 (`-i 127.0.0.1 -p 3278`).

```
FASTER.benchmark -b 0 -t 8 -r 50 -d zipf -i 127.0.0.1 -p 3278
```

Since the server is now loaded with data, you can re-run the benchmark without the setup (loading) phase using the 
`-s` option, as follows:

```
FASTER.benchmark -b 0 -t 8 -r 50 -d zipf -i 127.0.0.1 -p 3278 -s
```


## Variable-Length Server and Client

A more realistic and complex scenario is one where we want to store variable-length keys and values in FASTER, similar to
standard remote caches and key-value stores. Basically, we want the client to be able to read and write arbitrary sequences 
of bytes as keys and values, from and to the FASTER server.

On the server side, we instantiate a variable-length FasterKV store using the `SpanByte` concept of FASTER, which store variable-length
sequences of bytes as keys and values, inline in the hybrid log, without needing a separate object log. The data wire format is very
simple, as follows:

```
[ 4 byte payload length | payload ]
```

On the client side, our session API is based on `Memory<byte>`, which creates serializations that are binary compatible with 
the server. Thus, you can read and write arbitrary byte sequences to the server.

Code for variable-length keys and values are available at:

* Server: [cs/remote/samples/VarLenServer](https://github.com/microsoft/FASTER/tree/master/cs/remote/samples/VarLenServer)
* Client: [cs/remote/samples/VarLenClient](https://github.com/microsoft/FASTER/tree/master/cs/remote/samples/VarLenClient)

Note that like `FixedLenServer`, `VarLenServer` also accepts several command-line arguments to control the server memory 
utilization and other parameters. For example, you can run the server from the command line to listen on IP 
address 127.0.0.1 and port 3278 as follows:

```
> VarLenServer.exe --bind 127.0.0.1 --port 3278

FASTER variable-length KV server
Using page size of 32m
Using log memory size of 16g
There are 512 log pages in memory
Using disk segment size of 1g
Using hash index size of 64m (1m cache lines)
Started server
```
