---
title: "FasterKV Checkpointing and Recovery"
permalink: /docs/fasterkv-recovery/
excerpt: "FasterKV Checkpointing and Recovery"
last_modified_at: 2021-04-29
toc: true
---

## Checkpointing and Recovery

### Overall Summary

FASTER supports asynchronous non-blocking **checkpoint-based recovery**. Every new checkpoint persists (or makes durable) additional user-operations
(Read, Upsert or RMW). FASTER allows clients to keep track of operations that have persisted and those that have not using 
a session-based API.

This feature is based on a recovery model called Concurrent Prefix Recovery (CPR for short). You can read more about 
CPR in the research paper [here](https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf).
Briefly, CPR is based on (periodic) group commit. However, instead of using an expensive 
write-ahead log (WAL) which can kill FASTER's high performance, CPR: (1) provides a semantic description of committed
operations, of the form “all operations until offset Ti in session i”; and (2) uses asynchronous 
incremental checkpointing instead of a WAL to implement group commit in a scalable bottleneck-free manner.

Recall that each FASTER client starts a session, associated with a unique session ID (or name). All FASTER session operations
(Read, Upsert, RMW) carry a monotonic sequence number (sequence numbers are implicit in case of async calls). At any point in 
time, one may call the checkpointing API to initiate an asynchronous checkpoint of FASTER. After invoking the checkpoint, each FASTER 
session is (eventually) notified of a commit point. A commit point consists of (1) a sequence number, such that all operations
until, and no operations after, that sequence number, are guaranteed to be persisted as part of that checkpoint; (2) an optional
exception list of operations that were not part of the commit because they went pending and could not complete before the 
checkpoint, because the session was not active at the time of checkpointing.

The commit point information can be used by the session to clear any in-memory buffer of operations waiting to be performed. 
During recovery, sessions can continue using `ResumeSession` invoked with the same session ID. The function returns the thread-local 
sequence number until which that session hash been recovered. The new thread may use this information to replay all uncommitted 
operations since that point.

With async session operations on FASTER, operations return as soon as they complete, before commit. In order to wait for commit,
you simply issue an `await session.WaitForCommitAsync()` call. The call completes only after the operation is made persistent by
an asynchronous commit (checkpoint). The user is responsible for initiating the checkpoint asynchronously.

### Taking Checkpoints

A FASTER checkpoint consists of an optional index checkpoint, coupled with a later log 
checkpoint. FASTER first recovers the index and then replays the relevant part of the log
to get back to a consistent recovered state. If an index checkpoint is unavailable, FASTER
replays the entire log to reconstruct the index. An index checkpoint is taken as follows:

```cs
await store.TakeIndexCheckpointAsync();
```

FASTER supports two notions of log checkpointing: Snapshot and Fold-Over.

### Snapshot Checkpoint

This checkpoint is a full snapshot of in-memory portion of the hybrid log into a separate
snapshot file in the checkpoint folder. We recover using the main log followed by reading the
snapshot back into main memory to complete recovery. FASTER also supports incremental
snapshots, where the changes since the last full (or incremental) snapshot are captured into
a delta log file in the same folder as the base snapshot. This is specified using the 
`tryIncremental` parameter to the checkpoint operation.

```cs
await store.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot, tryIncremental: false);
```

### Fold-Over Checkpoint

A fold-over checkpoint simply flushes the main data log to disk, making it read-only, and
writes a small metadata file (`info.dat`) to the checkpoint folder. This is an incremental 
checkpoint by definition, as the mutable log consists of all changes since the previous 
fold-over checkpoint. FoldOver effectively moves the read-only marker of the hybrid log to 
the tail, and thus all the data is persisted as part of the same hybrid log (there is no 
separate snapshot file). 

All subsequent updates are written to new hybrid log tail locations, which gives Fold-Over 
its incremental nature. FoldOver is a very fast checkpointing scheme, but creates multiple 
versions of the data on the main log, which can increase the cost of garbage collection 
and take up main memory.

```cs
await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
```

### Full Checkpoint

You can take an index and log checkpoint together as follows:

```cs
await store.TakeFullCheckpointAsync(CheckpointType.FoldOver);
```

This is usually more expensive than log-only checkpoints as it needs to write the entire
hash table to disk. A preferred approach is to take frequent log-only checkpoints and
take an index checkpoint at coarse grained intervals in order to reduce recovery time.

### Checkpoint Management

By default, FASTER creates checkpoints in the folder specified using 
`CheckpointSettings.CheckpointDir`, with one folder per index or log checkpoint (each
as a unique Guid token). You can auto-purge old checkpoint as new ones are generated, by 
setting `CheckpointSettings.RemoveOutdated` to `true`. The last two index checkpoints 
and the last log checkpoint are kept. We keep the last two index checkpoints because the 
last index checkpoint may not be usable in case there is no subsequent log checkpoint
available. Make sure every index checkpoint is followed by at least one log checkpoint, for
the index checkpoint to be usable for recovery.

### Examples

You can find several checkpointing examples here:
* [StoreCheckpointRecover](https://github.com/microsoft/FASTER/tree/master/cs/samples/StoreCheckpointRecover)
* [ClassRecoveryDurablity](https://github.com/microsoft/FASTER/tree/master/cs/playground/ClassRecoveryDurability)
* [SumStore](https://github.com/microsoft/FASTER/tree/master/cs/playground/SumStore)
* [SimpleRecoveryTest](https://github.com/microsoft/FASTER/blob/master/cs/test/SimpleRecoveryTest.cs)
* [RecoveryChecks](https://github.com/microsoft/FASTER/blob/master/cs/test/RecoveryChecks.cs)

Below, we show a simple recovery example with asynchronous fold-over checkpointing.

```cs
public class PersistenceExample
{
    private FasterKV<long, long> fht;
    private IDevice log;

    public PersistenceExample()
    {
        log = Devices.CreateLogDevice("C:\\Temp\\hlog.log");
        fht = new FasterKV<long, long>(1L << 20, new LogSettings { LogDevice = log });
    }

    public void Run()
    {
        IssuePeriodicCheckpoints();
        RunSession();
    }

    public void Continue()
    {
        fht.Recover();
        IssuePeriodicCheckpoints();
        ContinueSession();
    }

    /* Helper Functions */
    private void RunSession()
    {
        using var session = fht.NewSession(new SimpleFunctions<long, long>(), "s1");
        long seq = 0; // sequence identifier

        long key = 1, input = 10;
        while (true)
        {
            key = (seq % 1L << 20);
            session.RMW(ref key, ref input, Empty.Default, seq++);
        }
    }

    private void ContinueSession()
    {
        using var session = fht.ResumeSession(new SimpleFunctions<long, long>(), "s1", out CommitPoint cp); // recovered session
        var seq = cp.UntilSerialNo + 1;

        long key = 1, input = 10;
        while (true)
        {
            key = (seq % 1L << 20);
            session.RMW(ref key, ref input, Empty.Default, seq++);
        }
    }

    private void IssuePeriodicCheckpoints()
    {
        var t = new Thread(() =>
        {
            while (true)
            {
                Thread.Sleep(10000);
                (_, _) = fht.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).GetAwaiter().GetResult();
            }
        });
        t.Start();
    }
}
```