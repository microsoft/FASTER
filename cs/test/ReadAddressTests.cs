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

            public Value(long first) => value = first;

            public override string ToString() => value.ToString();
        }

        public class Context
        {
            public RecordInfo recordInfo;
            public Status status;
        }

        /// <summary>
        /// Callback for FASTER operations
        /// </summary>
        public class Functions : SimpleFunctions<Key, Value, Context>
        {
            // Return false to force a chain of values.
            public override bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) => false;

            public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value) => false;

            // Track the recordInfo for its PreviousAddress.
            public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status, RecordInfo recordInfo)
            {
                if (!(ctx is null))
                {
                    ctx.recordInfo = recordInfo;
                    ctx.status = status;
                }
            }
        }
        private (FasterKV<Key, Value>, IDevice) CreateStore(bool useReadCache, bool copyReadsToTail, string testDir)
        {
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
            return (store, log);
        }

        private async static Task PopulateStore(FasterKV<Key, Value> store, bool useRMW)
        {
            // Start session with FASTER
            using var s = store.For(new Functions()).NewSession<Functions>();
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
                    s.RMW(ref key, ref value, serialNo: lap);
                else
                    s.Upsert(ref key, ref value, serialNo: lap);

                // Illustrate that deleted records can be shown as well (unless overwritten by in-place operations, which are not done here)
                if (lap == deleteLap)
                    s.Delete(ref key, serialNo: lap);
            }
        }

        private static bool ProcessRecord(FasterKV<Key, Value> store, Status status, RecordInfo recordInfo, int lap, ref Value actualOutput, ref int previousVersion)
        {
            Assert.GreaterOrEqual(lap, 0);
            long expectedValue = LapOffset(lap) + keyToScan;
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
        public void ReadAddressSyncTests(bool useReadCache, bool copyReadsToTail, bool useRMW)
        {
            var testDir = $"{TestContext.CurrentContext.TestDirectory}\\{TestContext.CurrentContext.Test.Name}";

            var (store, log) = CreateStore(useReadCache, copyReadsToTail, testDir);
            PopulateStore(store, useRMW).GetAwaiter().GetResult();

            using (var session = store.For(new Functions()).NewSession<Functions>())
            {
                // Two iterations to ensure no issues due to read-caching or copying to tail.
                for (int iteration = 0; iteration < 2; ++iteration)
                {
                    var output = default(Value);
                    var input = default(Value);
                    var key = new Key(keyToScan);
                    var context = new Context();
                    RecordInfo recordInfo = default;
                    int version = int.MaxValue;

                    for (int lap = 9; /* tested in loop */; --lap)
                    {
                        var status = session.Read(ref key, ref input, ref output, recordInfo.PreviousAddress, out recordInfo, context, serialNo: maxLap + 1);
                        if (status == Status.PENDING)
                        {
                            // This will spin CPU for each retrieved record; not recommended for performance-critical code or when retrieving chains for multiple records.
                            session.CompletePending(spinWait: true);
                            recordInfo = context.recordInfo;
                            status = context.status;
                        }
                        if (!ProcessRecord(store, status, recordInfo, lap, ref output, ref version))
                            break;
                    }
                }
            }

            // Clean up
            store.Dispose();
            log.Dispose();

            new DirectoryInfo(testDir).Delete(true);
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, false, false)]
        [TestCase(false, true, true)]
        [TestCase(true, false, false)]
        public async Task ReadAddressAsyncTests(bool useReadCache, bool copyReadsToTail, bool useRMW)
        {
            var testDir = $"{TestContext.CurrentContext.TestDirectory}\\{TestContext.CurrentContext.Test.Name}";

            var (store, log) = CreateStore(useReadCache, copyReadsToTail, testDir);
            await PopulateStore(store, useRMW);

            using (var session = store.For(new Functions()).NewSession<Functions>())
            {
                // Two iterations to ensure no issues due to read-caching or copying to tail.
                for (int iteration = 0; iteration < 2; ++iteration)
                {
                    var input = default(Value);
                    var key = new Key(keyToScan);
                    RecordInfo recordInfo = default;
                    int version = int.MaxValue;

                    for (int lap = 9; /* tested in loop */; --lap)
                    {
                        var readAsyncResult = await session.ReadAsync(ref key, ref input, recordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                        var (status, output) = readAsyncResult.Complete(out recordInfo);
                        if (!ProcessRecord(store, status, recordInfo, lap, ref output, ref version))
                            break;
                    }
                }
            }

            // Clean up
            store.Dispose();
            log.Dispose();

            new DirectoryInfo(testDir).Delete(true);
        }
    }
}
 