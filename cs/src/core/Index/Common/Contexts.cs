// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// </summary>
    public enum OperationType
    {
        /// <summary>
        /// </summary>
        READ,
        /// <summary>
        /// </summary>
        RMW,
        /// <summary>
        /// </summary>
        UPSERT,
        /// <summary>
        /// </summary>
        INSERT,
        /// <summary>
        /// </summary>
        DELETE
    }

    internal enum OperationStatus
    {
        SUCCESS,
        NOTFOUND,
        RETRY_NOW,
        RETRY_LATER,
        RECORD_ON_DISK,
        SUCCESS_UNMARK,
        CPR_SHIFT_DETECTED,
        CPR_PENDING_DETECTED
    }

    /// <summary>
    /// </summary>
    public class SerializedFasterExecutionContext
    {
        /// <summary>
        /// </summary>
        public int version;
        /// <summary>
        /// </summary>
        public long serialNum;
        /// <summary>
        /// </summary>
        public string guid;

        /// <summary>
        /// </summary>
        /// <param name="writer"></param>
        public void Write(StreamWriter writer)
        {
            writer.WriteLine(version);
            writer.WriteLine(guid);
            writer.WriteLine(serialNum);
        }

        /// <summary>
        /// </summary>
        /// <param name="reader"></param>
        public void Load(StreamReader reader)
        {
            string value = reader.ReadLine();
            version = int.Parse(value);

            guid = reader.ReadLine();
            value = reader.ReadLine();
            serialNum = long.Parse(value);
        }
    }

    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {

        /// <summary>
        /// </summary>
        public struct PendingContext
        {
            // User provided information
            /// <summary>
            /// </summary>
            public OperationType type;

            /// <summary>
            /// </summary>
            public IHeapContainer<Key> key;
            /// <summary>
            /// </summary>
            public IHeapContainer<Value> value;
            /// <summary>
            /// </summary>
            public Input input;
            /// <summary>
            /// </summary>
            public Output output;
            /// <summary>
            /// </summary>
            public Context userContext;

            // Some additional information about the previous attempt

            /// <summary>
            /// </summary>
            public long id;

            /// <summary>
            /// </summary>
            public int version;

            /// <summary>
            /// </summary>
            public long logicalAddress;

            /// <summary>
            /// </summary>
            public long serialNum;

            /// <summary>
            /// </summary>
            public HashBucketEntry entry;

            /// <summary>
            /// </summary>
            public void Dispose()
            {
                key?.Dispose();
                value?.Dispose();
            }
        }

        /// <inheritdoc />
        public class FasterExecutionContext : SerializedFasterExecutionContext
        {
            /// <summary>
            /// </summary>
            public Phase phase;
            /// <summary>
            /// </summary>
            public bool[] markers;
            /// <summary>
            /// </summary>
            public long totalPending;
            /// <summary>
            /// </summary>
            public Queue<PendingContext> retryRequests;
            /// <summary>
            /// </summary>
            public Dictionary<long, PendingContext> ioPendingRequests;
            /// <summary>
            /// </summary>
            public AsyncCountDown pendingReads;
            /// <summary>
            /// </summary>
            public AsyncQueue<AsyncIOContext<Key, Value>> readyResponses;
            /// <summary>
            /// </summary>
            public List<long> excludedSerialNos;

            /// <summary>
            /// </summary>
            public bool HasNoPendingRequests
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    return ioPendingRequests.Count == 0
                        && retryRequests.Count == 0;
                }
            }

            /// <summary>
            /// </summary>
            public FasterExecutionContext prevCtx;
        }
    }

 
    /// <summary>
    /// Descriptor for a CPR commit point
    /// </summary>
    public struct CommitPoint
    {
        /// <summary>
        /// Serial number until which we have committed
        /// </summary>
        public long UntilSerialNo;

        /// <summary>
        /// List of operation serial nos excluded from commit
        /// </summary>
        public List<long> ExcludedSerialNos;
    }

    /// <summary>
    /// Recovery info for hybrid log
    /// </summary>
    public struct HybridLogRecoveryInfo
    {
        /// <summary>
        /// Guid
        /// </summary>
        public Guid guid;
        /// <summary>
        /// Use snapshot file
        /// </summary>
        public int useSnapshotFile;
        /// <summary>
        /// Version
        /// </summary>
        public int version;
        /// <summary>
        /// Flushed logical address
        /// </summary>
        public long flushedLogicalAddress;
        /// <summary>
        /// Start logical address
        /// </summary>
        public long startLogicalAddress;
        /// <summary>
        /// Final logical address
        /// </summary>
        public long finalLogicalAddress;
        /// <summary>
        /// Head address
        /// </summary>
        public long headAddress;
        /// <summary>
        /// Begin address
        /// </summary>
        public long beginAddress;

        /// <summary>
        /// Commit tokens per session restored during Continue
        /// </summary>
        public ConcurrentDictionary<string, CommitPoint> continueTokens;

        /// <summary>
        /// Commit tokens per session created during Checkpoint
        /// </summary>
        public ConcurrentDictionary<string, CommitPoint> checkpointTokens;

        /// <summary>
        /// Object log segment offsets
        /// </summary>
        public long[] objectLogSegmentOffsets;

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="token"></param>
        /// <param name="_version"></param>
        public void Initialize(Guid token, int _version)
        {
            guid = token;
            useSnapshotFile = 0;
            version = _version;
            flushedLogicalAddress = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
            headAddress = 0;

            continueTokens = new ConcurrentDictionary<string, CommitPoint>();
            checkpointTokens = new ConcurrentDictionary<string, CommitPoint>();

            objectLogSegmentOffsets = null;
        }

        /// <summary>
        /// Initialize from stream
        /// </summary>
        /// <param name="reader"></param>
        public void Initialize(StreamReader reader)
        {
            continueTokens = new ConcurrentDictionary<string, CommitPoint>();

            string value = reader.ReadLine();
            guid = Guid.Parse(value);

            value = reader.ReadLine();
            useSnapshotFile = int.Parse(value);

            value = reader.ReadLine();
            version = int.Parse(value);

            value = reader.ReadLine();
            flushedLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            startLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            finalLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            headAddress = long.Parse(value);

            value = reader.ReadLine();
            beginAddress = long.Parse(value);

            value = reader.ReadLine();
            var numSessions = int.Parse(value);

            for (int i = 0; i < numSessions; i++)
            {
                var guid = reader.ReadLine();
                value = reader.ReadLine();
                var serialno = long.Parse(value);

                var exclusions = new List<long>();
                var exclusionCount = int.Parse(reader.ReadLine());
                for (int j = 0; j < exclusionCount; j++)
                    exclusions.Add(long.Parse(reader.ReadLine()));

                continueTokens.TryAdd(guid, new CommitPoint
                    {
                        UntilSerialNo = serialno,
                        ExcludedSerialNos = exclusions
                    });
            }

            // Read object log segment offsets
            value = reader.ReadLine();
            var numSegments = int.Parse(value);
            if (numSegments > 0)
            {
                objectLogSegmentOffsets = new long[numSegments];
                for (int i = 0; i < numSegments; i++)
                {
                    value = reader.ReadLine();
                    objectLogSegmentOffsets[i] = long.Parse(value);
                }
            }
        }

        /// <summary>
        ///  Recover info from token
        /// </summary>
        /// <param name="token"></param>
        /// <param name="checkpointManager"></param>
        /// <returns></returns>
        internal void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            var metadata = checkpointManager.GetLogCommitMetadata(token);
            if (metadata == null)
                throw new FasterException("Invalid log commit metadata for ID " + token.ToString());

            using StreamReader s = new StreamReader(new MemoryStream(metadata));
            Initialize(s);
        }

        /// <summary>
        /// Reset
        /// </summary>
        public void Reset()
        {
            Initialize(default, -1);
        }

        /// <summary>
        /// Write info to byte array
        /// </summary>
        public byte[] ToByteArray()
        {
            using MemoryStream ms = new MemoryStream();
            using (StreamWriter writer = new StreamWriter(ms))
            {
                writer.WriteLine(guid);
                writer.WriteLine(useSnapshotFile);
                writer.WriteLine(version);
                writer.WriteLine(flushedLogicalAddress);
                writer.WriteLine(startLogicalAddress);
                writer.WriteLine(finalLogicalAddress);
                writer.WriteLine(headAddress);
                writer.WriteLine(beginAddress);

                writer.WriteLine(checkpointTokens.Count);
                foreach (var kvp in checkpointTokens)
                {
                    writer.WriteLine(kvp.Key);
                    writer.WriteLine(kvp.Value.UntilSerialNo);
                    writer.WriteLine(kvp.Value.ExcludedSerialNos.Count);
                    foreach (long item in kvp.Value.ExcludedSerialNos)
                        writer.WriteLine(item);
                }

                // Write object log segment offsets
                writer.WriteLine(objectLogSegmentOffsets == null ? 0 : objectLogSegmentOffsets.Length);
                if (objectLogSegmentOffsets != null)
                {
                    for (int i = 0; i < objectLogSegmentOffsets.Length; i++)
                    {
                        writer.WriteLine(objectLogSegmentOffsets[i]);
                    }
                }
            }
            return ms.ToArray();
        }

        /// <summary>
        /// Print checkpoint info for debugging purposes
        /// </summary>
        public void DebugPrint()
        {
            Debug.WriteLine("******** HybridLog Checkpoint Info for {0} ********", guid);
            Debug.WriteLine("Version: {0}", version);
            Debug.WriteLine("Is Snapshot?: {0}", useSnapshotFile == 1);
            Debug.WriteLine("Flushed LogicalAddress: {0}", flushedLogicalAddress);
            Debug.WriteLine("Start Logical Address: {0}", startLogicalAddress);
            Debug.WriteLine("Final Logical Address: {0}", finalLogicalAddress);
            Debug.WriteLine("Head Address: {0}", headAddress);
            Debug.WriteLine("Begin Address: {0}", beginAddress);
            Debug.WriteLine("Num sessions recovered: {0}", continueTokens.Count);
            Debug.WriteLine("Recovered sessions: ");
            foreach (var sessionInfo in continueTokens)
            {
                Debug.WriteLine("{0}: {1}", sessionInfo.Key, sessionInfo.Value);
            }
        }
    }

    internal struct HybridLogCheckpointInfo
    {
        public HybridLogRecoveryInfo info;
        public IDevice snapshotFileDevice;
        public IDevice snapshotFileObjectLogDevice;
        public SemaphoreSlim flushedSemaphore;
        public long started;

        public void Initialize(Guid token, int _version, ICheckpointManager checkpointManager)
        {
            info.Initialize(token, _version);
            started = 0;
            checkpointManager.InitializeLogCheckpoint(token);
        }

        public void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            info.Recover(token, checkpointManager);
            started = 0;
        }

        public void Reset()
        {
            started = 0;
            flushedSemaphore = null;
            info.Reset();
            if (snapshotFileDevice != null) snapshotFileDevice.Close();
            if (snapshotFileObjectLogDevice != null) snapshotFileObjectLogDevice.Close();
        }
    }

    internal struct IndexRecoveryInfo
    {
        public Guid token;
        public long table_size;
        public ulong num_ht_bytes;
        public ulong num_ofb_bytes;
        public int num_buckets;
        public long startLogicalAddress;
        public long finalLogicalAddress;

        public void Initialize(Guid token, long _size)
        {
            this.token = token;
            table_size = _size;
            num_ht_bytes = 0;
            num_ofb_bytes = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
            num_buckets = 0;
        }
        public void Initialize(StreamReader reader)
        {
            string value = reader.ReadLine();
            token = Guid.Parse(value);

            value = reader.ReadLine();
            table_size = long.Parse(value);

            value = reader.ReadLine();
            num_ht_bytes = ulong.Parse(value);

            value = reader.ReadLine();
            num_ofb_bytes = ulong.Parse(value);

            value = reader.ReadLine();
            num_buckets = int.Parse(value);

            value = reader.ReadLine();
            startLogicalAddress = long.Parse(value);

            value = reader.ReadLine();
            finalLogicalAddress = long.Parse(value);
        }

        public void Recover(Guid guid, ICheckpointManager checkpointManager)
        {
            var metadata = checkpointManager.GetIndexCommitMetadata(guid);
            if (metadata == null)
                throw new FasterException("Invalid index commit metadata for ID " + guid.ToString());
            using StreamReader s = new StreamReader(new MemoryStream(metadata));
            Initialize(s);
        }

        public byte[] ToByteArray()
        {
            using MemoryStream ms = new MemoryStream();
            using (var writer = new StreamWriter(ms))
            {

                writer.WriteLine(token);
                writer.WriteLine(table_size);
                writer.WriteLine(num_ht_bytes);
                writer.WriteLine(num_ofb_bytes);
                writer.WriteLine(num_buckets);
                writer.WriteLine(startLogicalAddress);
                writer.WriteLine(finalLogicalAddress);
            }
            return ms.ToArray();
        }

        public void DebugPrint()
        {
            Debug.WriteLine("******** Index Checkpoint Info for {0} ********", token);
            Debug.WriteLine("Table Size: {0}", table_size);
            Debug.WriteLine("Main Table Size (in GB): {0}", ((double)num_ht_bytes) / 1000.0 / 1000.0 / 1000.0);
            Debug.WriteLine("Overflow Table Size (in GB): {0}", ((double)num_ofb_bytes) / 1000.0 / 1000.0 / 1000.0);
            Debug.WriteLine("Num Buckets: {0}", num_buckets);
            Debug.WriteLine("Start Logical Address: {0}", startLogicalAddress);
            Debug.WriteLine("Final Logical Address: {0}", finalLogicalAddress);
        }
        public void Reset()
        {
            token = default;
            table_size = 0;
            num_ht_bytes = 0;
            num_ofb_bytes = 0;
            num_buckets = 0;
            startLogicalAddress = 0;
            finalLogicalAddress = 0;
        }
    }

    internal struct IndexCheckpointInfo
    {
        public IndexRecoveryInfo info;
        public IDevice main_ht_device;

        public void Initialize(Guid token, long _size, ICheckpointManager checkpointManager)
        {
            info.Initialize(token, _size);
            checkpointManager.InitializeIndexCheckpoint(token);
            main_ht_device = checkpointManager.GetIndexDevice(token);
        }
        public void Recover(Guid token, ICheckpointManager checkpointManager)
        {
            info.Recover(token, checkpointManager);
        }
        public void Reset()
        {
            info.Reset();
            main_ht_device.Close();
        }
    }
}
