// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;

namespace FASTER.test.readaddress
{
    [TestFixture]
    public class ReadAddressTests
    {
        const int numKeys = 1000;
        const int keyMod = 100;
        const int maxLap = numKeys / keyMod;
        const int deleteLap = maxLap / 2;
        const int keyToScan = 42;

        private static int LapOffset(int lap) => lap * numKeys * 100;

        public struct Key
        {
            public long key;

            public Key(long first) => key = first;

            public override string ToString() => key.ToString();

            internal class Comparer : IFasterEqualityComparer<Key>
            {
                public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.key);

                public bool Equals(ref Key k1, ref Key k2) => k1.key == k2.key;
            }
        }

        public struct Value
        {
            public long value;

            public Value(long value) => this.value = value;

            public override string ToString() => value.ToString();
        }

        public class Context
        {
            public Value output;
            public RecordInfo recordInfo;
            public Status status;

            public void Reset()
            {
                this.output = default;
                this.recordInfo = default;
                this.status = Status.OK;
            }
        }

        private static long SetReadOutput(long key, long value) => (key << 32) | value;

        /// <summary>
        /// Callback for FASTER operations
        /// </summary>
        public class Functions : AdvancedSimpleFunctions<Key, Value, Context>
        {
            public override void ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst, long address) 
                => dst.value = SetReadOutput(key.key, value.value);

            public override void SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst, long address) 
                => dst.value = SetReadOutput(key.key, value.value);

            // Return false to force a chain of values.
            public override bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, long address) => false;

            public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, long address) => false;

            // Track the recordInfo for its PreviousAddress.
            public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status, RecordInfo recordInfo)
            {
                if (!(ctx is null))
                {
                    ctx.output = output;
                    ctx.recordInfo = recordInfo;
                    ctx.status = status;
                }
            }
        }

        private class TestStore : IDisposable
        {
            internal FasterKV<Key, Value> fkv;
            internal IDevice log;
            internal string dirName;

            internal TestStore(FasterKV<Key, Value> fkv, IDevice log, string dirName)
            {
                this.fkv = fkv;
                this.log = log;
                this.dirName = dirName;
            }

            public void Dispose()
            {
                if (!(fkv is null))
                    fkv.Dispose();
                if (!(log is null))
                    log.Dispose();
                if (!string.IsNullOrEmpty(dirName))
                    new DirectoryInfo(dirName).Delete(true);
            }
        }

        private TestStore CreateStore(bool useReadCache, bool copyReadsToTail)
        {
            var testDir = $"{TestContext.CurrentContext.TestDirectory}\\{TestContext.CurrentContext.Test.Name}";
            var log = Devices.CreateLogDevice($"{testDir}\\hlog.log");

            var logSettings = new LogSettings
            {
                LogDevice = log,
                ObjectLogDevice = new NullDevice(),
                ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null,
                CopyReadsToTail = copyReadsToTail,
                // Use small-footprint values
                PageSizeBits = 12, // (4K pages)
                MemorySizeBits = 20 // (1M memory for main log)
            };

            var store = new FasterKV<Key, Value>(
                size: 1L << 20,
                logSettings: logSettings,
                checkpointSettings: new CheckpointSettings { CheckpointDir = $"{testDir}\\CheckpointDir" },
                serializerSettings: null,
                comparer: new Key.Comparer()
                );
            return new TestStore(store, log, testDir);
        }

        private async static Task PopulateStore(FasterKV<Key, Value> store, bool useRMW)
        {
            using var session = store.For(new Functions()).NewSession<Functions>();
            Console.WriteLine($"Writing {numKeys} keys to FASTER", numKeys);

            var prevLap = 0;
            for (int ii = 0; ii < numKeys; ii++)
            {
                // lap is used to illustrate the changing values
                var lap = ii / keyMod;

                if (lap != prevLap)
                {
                    await store.TakeFullCheckpointAsync(CheckpointType.FoldOver);
                    prevLap = lap;
                }

                var key = new Key(ii % keyMod);

                var value = new Value(key.key + LapOffset(lap));
                if (useRMW)
                    session.RMW(ref key, ref value, serialNo: lap);
                else
                    session.Upsert(ref key, ref value, serialNo: lap);

                // Illustrate that deleted records can be shown as well (unless overwritten by in-place operations, which are not done here)
                if (lap == deleteLap)
                    session.Delete(ref key, serialNo: lap);
            }
        }

        private static bool ProcessRecord(FasterKV<Key, Value> store, Status status, RecordInfo recordInfo, int lap, ref Value actualOutput, ref int previousVersion)
        {
            Assert.GreaterOrEqual(lap, 0);
            long expectedValue = SetReadOutput(keyToScan, LapOffset(lap) + keyToScan);

            Assert.AreEqual(status == Status.NOTFOUND, recordInfo.Tombstone, $"status({status}) == NOTFOUND != Tombstone ({recordInfo.Tombstone})");
            Assert.AreEqual(lap == deleteLap, recordInfo.Tombstone, $"lap({lap}) == deleteLap({deleteLap}) != Tombstone ({recordInfo.Tombstone})");
            Assert.GreaterOrEqual(previousVersion, recordInfo.Version);
            if (!recordInfo.Tombstone)
                Assert.AreEqual(expectedValue, actualOutput.value);

            // Check for end of loop
            previousVersion = recordInfo.Version;
            return recordInfo.PreviousAddress >= store.Log.BeginAddress;
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, false, false)]
        [TestCase(false, true, true)]
        [TestCase(true, false, false)]
        public void VersionedReadSyncTests(bool useReadCache, bool copyReadsToTail, bool useRMW)
        {
            using var testStore = CreateStore(useReadCache, copyReadsToTail);
            PopulateStore(testStore.fkv, useRMW).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Value);
                var input = default(Value);
                var key = new Key(keyToScan);
                var context = new Context();
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var status = session.Read(ref key, ref input, ref output, ref recordInfo, context, serialNo: maxLap + 1);
                    if (status == Status.PENDING)
                    {
                        // This will spin CPU for each retrieved record; not recommended for performance-critical code or when retrieving chains for multiple records.
                        session.CompletePending(spinWait: true);
                        output = context.output;
                        recordInfo = context.recordInfo;
                        status = context.status;
                        context.Reset();
                    }
                    if (!ProcessRecord(testStore.fkv, status, recordInfo, lap, ref output, ref version))
                        break;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, false, false)]
        [TestCase(false, true, true)]
        [TestCase(true, false, false)]
        public async Task VersionedReadAsyncTests(bool useReadCache, bool copyReadsToTail, bool useRMW)
        {
            using var testStore = CreateStore(useReadCache, copyReadsToTail);
            await PopulateStore(testStore.fkv, useRMW);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(keyToScan);
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordInfo);
                    if (!ProcessRecord(testStore.fkv, status, recordInfo, lap, ref output, ref version))
                        break;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, false, false)]
        [TestCase(false, true, true)]
        [TestCase(true, false, false)]
        public void ReadAtAddressSyncTests(bool useReadCache, bool copyReadsToTail, bool useRMW)
        {
            using var testStore = CreateStore(useReadCache, copyReadsToTail);
            PopulateStore(testStore.fkv, useRMW).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Value);
                var input = default(Value);
                var key = new Key(keyToScan);
                var context = new Context();
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordInfo.PreviousAddress;

                    var status = session.Read(ref key, ref input, ref output, ref recordInfo, context, serialNo: maxLap + 1);
                    if (status == Status.PENDING)
                    {
                        // This will spin CPU for each retrieved record; not recommended for performance-critical code or when retrieving chains for multiple records.
                        session.CompletePending(spinWait: true);
                        output = context.output;
                        recordInfo = context.recordInfo;
                        status = context.status;
                        context.Reset();
                    }
                    if (!ProcessRecord(testStore.fkv, status, recordInfo, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordInfo = recordInfo;

                        status = session.ReadAtAddress(readAtAddress, ref input, ref output, context, serialNo: maxLap + 1);
                        if (status == Status.PENDING)
                        {
                            // This will spin CPU for each retrieved record; not recommended for performance-critical code or when retrieving chains for multiple records.
                            session.CompletePending(spinWait: true);
                            output = context.output;
                            recordInfo = context.recordInfo;
                            status = context.status;
                            context.Reset();
                        }

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordInfo, recordInfo);
                    }
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, false, false)]
        [TestCase(false, true, true)]
        [TestCase(true, false, false)]
        public async Task ReadAtAddressAsyncTests(bool useReadCache, bool copyReadsToTail, bool useRMW)
        {
            using var testStore = CreateStore(useReadCache, copyReadsToTail);
            await PopulateStore(testStore.fkv, useRMW);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(keyToScan);
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordInfo.PreviousAddress;

                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordInfo);
                    if (!ProcessRecord(testStore.fkv, status, recordInfo, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordInfo = recordInfo;

                        readAsyncResult = await session.ReadAtAddressAsync(readAtAddress, ref input, default, serialNo: maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordInfo);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordInfo, recordInfo);
                    }
                }
            }
        }
    }
}
