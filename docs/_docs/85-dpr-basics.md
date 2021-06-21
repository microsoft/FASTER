---
title: "LibDPR - Basics"
permalink: /docs/dpr-basics/
excerpt: "LibDPR Basics"
last_modified_at: 2021-06-18
toc: false
classes: wide
---
DPR stands for Distributed Prefix Recovery --- it ensures that client sessions can
issue operations across failure domains (e.g. multiple FASTER shards running), but
still get the same prefix recoverability guarantees as if they are working with 
a single machine. ([see our paper for details](https://tli2.github.io/assets/pdf/dpr-sigmod2021.pdf))
We use DPR to add prefix recoverability to a sharded remote-FASTER cluster, but
DPR is generalizable beyond FASTER. LibDPR is our solution to provide a set of tools
that encapsulate much of the complexity involved with implementing DPR, so you can
add it to your system without hassle.

## Overview
LibDPR is a layer above cache-stores -- linearizable storage shards that
can process operations quickly in-memory, and asynchronously persist them either
through writes to storage or some other means (e.g. FASTER). LibDPR helps users
assemble prefix-recoverable distributed cluster from single-machine implementations
quickly while hiding most of the complexity. You can find a simple example on 
how to do so under `samples/DprCounters`.

## SimpleStateObject
`StateObject`s are single shards that span cache and storage. LibDPR interacts with
underlying client systems (e.g., a counter, FASTER K-V, Redis) through the
`IStateObject` API. The easiest way to add DPR to an existing system would be 
through the `SimpleStateObject` class. For more advanced usage please read the
detailed guide.

To define a new state object for DPR. extend from the `SimpleStateObject` class
and implement the two abstract methods of `PerformCheckpoint` and `RestoreCheckpoint`.
`SimpleStateObject` provides mechanisms to correctly synchronize modifications to
the state object and invocations of the two methods, so it is ok to assume that
both of these methods run in isolation for the entirety of their invocation.

Here's an example implementation of the two methods. You can find the full
example under `samples/DprCounters`.

```C#
// With SimpleStateObject, CounterStateObject only needs to implement a single-threaded
// checkpoint scheme.
protected override void PerformCheckpoint(long version, Action onPersist)
{
    // Use a simple naming scheme to associate checkpoints with versions. A more sophisticated scheme may
    // store persistent mappings or use other schemes to do so.
    var fileName = Path.Join(checkpointDirectory, version.ToString());
    var fs = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);

    // libDPR will ensure that request batches that are protected with VersionScheme.Enter() and
    // VersionScheme.Leave() will not interleave with checkpoint or recovery code. It is therefore safe
    // to read and write values without protection in this function
    prevCounters[version] = value;
    
    // Once the content of the checkpoint is established (we have read a current snapshot of value), it is ok
    // to write to disk asynchronously and allow other operations to continue. In SimpleStateObject, 
    // operations are blocked before PerformCheckpoint return.
    fs.WriteAsync(BitConverter.GetBytes(value), 0, sizeof(long)).ContinueWith(token =>
    {
        if (!token.IsCompletedSuccessfully)
            Console.WriteLine($"Error {token} during checkpoint");
        // We need to invoke onPersist() to inform DPR when a checkpoint is on disk
        onPersist();
        fs.Dispose();
    });
}
```

```C#
// With SimpleStateObject, CounterStateObject can just implement a single-threaded blocking recovery function
protected override void RestoreCheckpoint(long version)
{
    // RestoreCheckpoint is only called on machines that did not physically go down (otherwise they will simply
    // load the surviving version on restart). libDPR will additionally never request a worker to restore
    // checkpoints earlier than the committed version in the DPR cut. We can therefore rely on a (relatively
    // small) stash of in-memory snapshots to quickly handle this call.
    value = prevCounters[version];
    
    // Remove any cached versions larger than the restored ones because those are rolled back.
    PruneCachedVersions(v => v > version);
}
```

## Server-side DPR

Now that we have implemented `IStateObject`, libDPR will be able to invoke checkpoints and recovery
when necessary. All that remains is to perform DPR dependency tracking during normal operations. LibDPR tracking
works on message batch granularity -- libDPR must be informed once before the state object processes the 
request, and another time before the state object sends back any results. Other than these two method invocations,
libDPR does not mediate between the state object and requests, and developers are free to do what they want between
the two method calls. The `DprServer` class exists as the central hub for all server-side
DPR method calls.

Here's an example from `CounterServer`:

```C#
// Before executing server-side logic, check with DPR to start tracking for the batch and make sure 
// we are allowed to execute it. If not, the response header will be populated and we should immediately
// return that to the client side libDPR.
if (dprServer.RequestBatchBegin(ref request, ref response, out var tracker))
{
    // If so, protect the execution and obtain the version this batch will execute in
    var v = dprServer.StateObject().VersionScheme().Enter();
    // Add operation to version tracking using the libDPR-supplied version tracker
    tracker.MarkOneOperationVersion(0, v);
    
    // Execute the request batch. In this case, always a single increment operation.
    result = dprServer.StateObject().value;
    dprServer.StateObject().value +=
        BitConverter.ToInt64(new Span<byte>(inBuffer, sizeof(int) + size - sizeof(long), sizeof(long)));
    
    // Once requests are done executing, stop protecting this batch so DPR can progress
    dprServer.StateObject().VersionScheme().Leave();
    // Signal the end of execution for DPR to finish up and populate a response header
    responseHeaderSize = dprServer.SignalBatchFinish(ref request, responseBuffer, tracker);
}
```

Client-side and Server-side libDPR will issue bytes of DPR headers that encode tracking information, and it's
the developer's job to deliver them as part of their network protocol. Once delivered, however, the developer
only needs to forward them to the appropriate libDPR method. In the above example, we can see
that before user code executes the counter logic, it first request to begin processing of the
batch with the local `DprServer` object with the DPR request header. `DprServer` may reject
this request and populate a response header in case of failures that require handling. In that case,
the batch should not be executed, but the response header should be returned to the client to trigger
error handling. Assuming the request has been successfully granted however, the method hands out a 
`tracker` object. Developers are expected to mark each operation with the version they are completed in,
corresponding to the checkpoint they will be recovered as a part of. For state objects that
derive from `SimpleStateObject`, libDPR provides a default scheme for doing so. Before batch
execution, call `Enter` to obtain a version number that every operation in the batch will be processed in.
This is similar to a lock that prevents checkpoints until all of its member operations are done.
After batch execution, call `Leave` to stop protection and allow checkpoints to proceed. At the end of processing,
invoke `SignalBatchFinish` to obtain a DPR response header to return to the client.

## Client-side DPR
Client-side DPR works in a similar way, with `DprClientSession` objects handing out DPR request
headers and consuming returned DPR response headers. Here's an example:

```C#
// Before sending operations, consult with DPR client for a batch header. For this simple example, we 
// are using one message per batch
var seqNo = session.IssueBatch(1, worker, out var header);
// send the request
var success = session.ResolveBatch(ref response);
// if successful, safe to expose operation result to client. Otherwise, must discard this message.
```

In addition, developers can check for operation status in `DprClientSession` using the 
`GetCommitPoint()` call, where operations are represented by the sequence numbers returned from
`IssueBatch` (in the case of a batch with more than 1 operation, the operations are numbered contiguously
in the range [seqNo, batchSize)). Note that `GetCommitPoint()` may be computationally expensive if not
called regularly. Developers can choose to turn off commit tracking to reduce overhead if they do not need
operation-level commit status tracking.

## DPR Finders

Under the hood, both DPR clients and servers asynchronously communicate with a backend service we call the
DPR Finder to report their status and receive updates about the rest of the cluster. Think of it as zookeeper for
commit guarantees, except much more specialized and lightweight. We provide the `GraphDprFinder` implementation
and an `IDprFinder` interface that allows for custom-built DPR Finders.

To launch the a DPR Finder service, check out `GraphDprFinderServer`, a fault-tolerant implementation that persists
any guarantees to storage devices. The cluster can tolerate DPR Finder failures as long as a new DPR Finder is 
started on the same storage devices used by the failed one, and network traffic is correctly rerouted (e.g., by
restarting it in kubernetes). To communicate with the DPR finder backend, see `GraphDprFinder`. Here's an example
on how one might start a DPR Finder service and talk to it:

```C#
 // Use a simple pair of in-memory storage to back our DprFinder server for now. Start a local DPRFinder
// server for the cluster. For real deployments, you can replace with storage devices that will survive failures (e.g.
// Azure Blob storage, Kubernetes persistent volumes)
var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
var device = new PingPongDevice(localDevice1, localDevice2);
using var dprFinderServer = new GraphDprFinderServer("localhost", 15721, new GraphDprFinderBackend(device));
dprFinderServer.StartServer();

// ...
// Now you can create GraphDprFinder objects for use with DPR servers and clients. Beware that a DprFinder does not 
// automatically communicate with the rest of the cluster, and will only do so when refresh is explicitly called.
var finder = new GraphDprFinder("localhost", 15721);
```

