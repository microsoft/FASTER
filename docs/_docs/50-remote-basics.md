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

You can create and deploy a server based on either fixed-size (struct) Key and Value types or variable-size 
(varlen) Key and Value types. You can customize the Input and Output as well as other FASTER settings such
as hash table size, log size, etc. before deploying the server. `FASTER.server` is the library used to 
create a customized server, and takes an instantiated FASTER store as argument during its creation. We 
provide two example servers out of the box. The server is deployed to listen on a specific IP address and 
port. The default FASTER port is 3278. Right now, the server communicates using a binary protocol, but we
are also adding support for the RESP protocol so that RESP clients (Redis) in any language can communicate 
with FASTER servers.


## Using FasterKV client in remote application

Use `FASTER.client` as a client library in C# to connect to any server and run operations remotely. The library
exposes the ability to create a session, very similar to `ClientSession` in embedded deployments. A session targets
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
[cs/remote/samples/FixedLenServer](https://github.com/microsoft/FASTER/tree/master/cs/remote/samples/FixedLenServer).

Here, we first create a FasterKV instance, recovering from a checkpoint if needed:

```cs
var store = new FasterKV<Key, Value>(indexSize, logSettings, checkpointSettings);
```

Next, we construct the FasterKVServer as follows, specifying the IP address to bind, and the port number to listen on. Types 
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
   value.value += input.value;
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

Note that `VarLenServer` accepts several command-line arguments to easily control the server memory utilization and 
other parameters.


### Client Code

A sample client application that talks to the above server is available at 
[cs/remote/samples/FixedLenClient](https://github.com/microsoft/FASTER/tree/master/cs/remote/samples/FixedLenClient). It
starts simply by defining the client instance that specifies the server IP address and port to connect to:

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
the client will invoke as operations complete.


### Sync Client API

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

Below we show two RMW operations and a Read to verify that the RMW operations succeeded:

```cs
session.RMW(23, 25);
session.RMW(23, 25);
session.CompletePending(true);
```

A subsequent read will produce a result that is `25+25` more than the older value of `10023` for key `23`.


### Async Client API

In `AsyncSamples` we use an asynchronous API to communicate with the server. For example, we perform an async read
as follows:

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
necessary information such as server IP address and port. Here is a sample run that runs the benchark with one session (`-t 1`), 
no NUMA sharding (`-n 0`), 50% reads (`-r 50`), uniform distribution (`-d uniform`), connecting to IP address 127.0.0.1
on port 3278 (`-i 127.0.0.1 -p 3278`). Make sure `FixedLenServer` is first listening on the same address and port.

```
FASTER.benchmark -b 0 -t 1 -n 0 -r 50 -d uniform -i 127.0.0.1 -p 3278
```

## Variable-Length Server and Client

A more complex scenario is one where we want to store variable-length keys and values in FASTER. Basically, we want
the client to be able to read and write arbitrary sequences of bytes to the FASTER server. For this, we instantiate
a variable-length FasterKV store on the server side, using the `SpanByte` concept of FASTER to store the keys and
values without inline without needing a separate object log. The data wire format is as follows:

```
[ 4 byte payload length | payload ]
```

On the client side, we can support a session API based on `Memory<byte>`, which creates serializations that are
binary compatible with the server. The examples are available at:

* Server: [cs/remote/samples/VarLenServer](https://github.com/microsoft/FASTER/tree/master/cs/remote/samples/VarLenServer)
* Client: [cs/remote/samples/VarLenClient](https://github.com/microsoft/FASTER/tree/master/cs/remote/samples/VarLenClient)

Note that `VarLenServer` accepts several command-line arguments to easily control the server memory utilization and 
other parameters.
