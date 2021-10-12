// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
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
        const int defaultKeyToScan = 42;

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

        private static long SetReadOutput(long key, long value) => (key << 32) | value;

        internal class Functions : SimpleFunctions<Key, Value>
        {
            internal long lastWriteAddress = Constants.kInvalidAddress;

            public override bool ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref RecordInfo recordInfo, long address)
            {
                dst.value = SetReadOutput(key.key, value.value);
                return true;
            }

            public override bool SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref RecordInfo recordInfo, long address)
            {
                dst.value = SetReadOutput(key.key, value.value);
                return true;
            }

            // Return false to force a chain of values.
            public override bool ConcurrentWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address) => false;

            public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RecordInfo recordInfo, long address) => false;

            // Record addresses
            public override void SingleWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address)
            {
                this.lastWriteAddress = address;
                base.SingleWriter(ref key, ref input, ref src, ref dst, ref recordInfo, address);
            }

            public override void InitialUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RecordInfo recordInfo, long address)
            {
                this.lastWriteAddress = address;
                base.InitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
            }

            public override void CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Value output, ref RecordInfo recordInfo, long address)
            {
                this.lastWriteAddress = address;
                base.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, address);
            }

            // Track the recordInfo for its PreviousAddress.
            public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

            public override void RMWCompletionCallback(ref Key key, ref Value input, ref Value output, Empty ctx, Status status)
            {
                output = input;
                base.RMWCompletionCallback(ref key, ref input, ref output, ctx, status);
            }
        }

        private class TestStore : IDisposable
        {
            internal FasterKV<Key, Value> fkv;
            internal IDevice logDevice;
            internal string testDir;
            private readonly bool flush;

            internal long[] InsertAddresses = new long[numKeys];

            internal TestStore(bool useReadCache, CopyReadsToTail copyReadsToTail, bool flush)
            {
                this.testDir = TestUtils.MethodTestDir;
                TestUtils.DeleteDirectory(this.testDir, wait:true);
                this.logDevice = Devices.CreateLogDevice($"{testDir}/hlog.log");
                this.flush = flush;

                var logSettings = new LogSettings
                {
                    LogDevice = logDevice,
                    ObjectLogDevice = new NullDevice(),
                    ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null,
                    CopyReadsToTail = copyReadsToTail,
                    // Use small-footprint values
                    PageSizeBits = 12, // (4K pages)
                    MemorySizeBits = 20 // (1M memory for main log)
                };

                this.fkv = new FasterKV<Key, Value>(
                    size: 1L << 20,
                    logSettings: logSettings,
                    checkpointSettings: new CheckpointSettings { CheckpointDir = $"{this.testDir}/CheckpointDir" },
                    serializerSettings: null,
                    comparer: new Key.Comparer()
                    );
            }

            internal async ValueTask Flush()
            {
                if (this.flush)
                {
                    if (!this.fkv.UseReadCache)
                        await this.fkv.TakeFullCheckpointAsync(CheckpointType.FoldOver);
                    this.fkv.Log.FlushAndEvict(wait: true);
                }
            }

            internal async Task Populate(bool useRMW, bool useAsync)
            {
                var functions = new Functions();
                using var session = this.fkv.For(functions).NewSession<Functions>();

                var prevLap = 0;
                for (int ii = 0; ii < numKeys; ii++)
                {
                    // lap is used to illustrate the changing values
                    var lap = ii / keyMod;

                    if (lap != prevLap)
                    {
                        await Flush();
                        prevLap = lap;
                    }

                    var key = new Key(ii % keyMod);
                    var value = new Value(key.key + LapOffset(lap));

                    var status = useRMW
                        ? useAsync
                            ? (await session.RMWAsync(ref key, ref value, serialNo: lap)).Complete().status
                            : session.RMW(ref key, ref value, serialNo: lap)
                        : session.Upsert(ref key, ref value, serialNo: lap);

                    if (status == Status.PENDING)
                        await session.CompletePendingAsync();

                    InsertAddresses[ii] = functions.lastWriteAddress;
                    //Assert.IsTrue(session.ctx.HasNoPendingRequests);

                    // Illustrate that deleted records can be shown as well (unless overwritten by in-place operations, which are not done here)
                    if (lap == deleteLap)
                        session.Delete(ref key, serialNo: lap);
                }

                await Flush();
            }

            internal bool ProcessChainRecord(Status status, RecordMetadata recordMetadata, int lap, ref Value actualOutput, ref int previousVersion)
            {
                var recordInfo = recordMetadata.RecordInfo;
                Assert.GreaterOrEqual(lap, 0);
                long expectedValue = SetReadOutput(defaultKeyToScan, LapOffset(lap) + defaultKeyToScan);

                Assert.AreEqual(status == Status.NOTFOUND, recordInfo.Tombstone, $"status({status}) == NOTFOUND != Tombstone ({recordInfo.Tombstone})");
                Assert.AreEqual(lap == deleteLap, recordInfo.Tombstone, $"lap({lap}) == deleteLap({deleteLap}) != Tombstone ({recordInfo.Tombstone})");
                Assert.GreaterOrEqual(previousVersion, recordInfo.Version);
                if (!recordInfo.Tombstone)
                    Assert.AreEqual(expectedValue, actualOutput.value);

                // Check for end of loop
                previousVersion = recordInfo.Version;
                return recordInfo.PreviousAddress >= this.fkv.Log.BeginAddress;
            }

            internal static void ProcessNoKeyRecord(Status status, ref Value actualOutput, int keyOrdinal)
            {
                if (status != Status.NOTFOUND)
                {
                    var keyToScan = keyOrdinal % keyMod;
                    var lap = keyOrdinal / keyMod;
                    long expectedValue = SetReadOutput(keyToScan, LapOffset(lap) + keyToScan);
                    Assert.AreEqual(expectedValue, actualOutput.value);
                }
            }

            public void Dispose()
            {
                this.fkv?.Dispose();
                this.fkv = null;
                this.logDevice?.Dispose();
                this.logDevice = null;
                TestUtils.DeleteDirectory(this.testDir);
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public void VersionedReadSyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            testStore.Populate(useRMW, useAsync:false).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Value);
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var status = session.Read(ref key, ref input, ref output, ref recordMetadata, serialNo: maxLap + 1);
                    if (status == Status.PENDING)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = TestUtils.GetSinglePendingResult(completedOutputs, out recordMetadata);
                    }
                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output, ref version))
                        break;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task VersionedReadAsyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordMetadata.RecordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);
                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output, ref version))
                        break;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public void ReadAtAddressSyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            testStore.Populate(useRMW, useAsync: false).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Value);
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordMetadata.RecordInfo.PreviousAddress;

                    var status = session.Read(ref key, ref input, ref output, ref recordMetadata, serialNo: maxLap + 1);
                    if (status == Status.PENDING)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = TestUtils.GetSinglePendingResult(completedOutputs, out recordMetadata);
                    }
                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordMetadata = recordMetadata;

                        status = session.ReadAtAddress(readAtAddress, ref input, ref output, serialNo: maxLap + 1);
                        if (status == Status.PENDING)
                        {
                            // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            (status, output) = TestUtils.GetSinglePendingResult(completedOutputs, out recordMetadata);
                        }

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordMetadata.RecordInfo, recordMetadata.RecordInfo);
                    }
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadAtAddressAsyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordMetadata.RecordInfo.PreviousAddress;

                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordMetadata.RecordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);
                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordMetadata = recordMetadata;

                        readAsyncResult = await session.ReadAtAddressAsync(readAtAddress, ref input, default, serialNo: maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordMetadata);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordMetadata.RecordInfo, recordMetadata.RecordInfo);
                    }
                }
            }
        }

        // Test is similar to others but tests the Overload where RadFlag.none is set -- probably don't need all combinations of test but doesn't hurt 
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadAtAddressAsyncReadFlagsNoneTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordMetadata.RecordInfo.PreviousAddress;

                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordMetadata.RecordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);
                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordMetadata = recordMetadata;

                        readAsyncResult = await session.ReadAtAddressAsync(readAtAddress, ref input, ReadFlags.None, default, serialNo: maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordMetadata);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordMetadata.RecordInfo, recordMetadata.RecordInfo);
                    }
                }
            }
        }

        // Test is similar to others but tests the Overload where RadFlag.SkipReadCache is set
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadAtAddressAsyncReadFlagsSkipCacheTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordMetadata.RecordInfo.PreviousAddress;

                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordMetadata.RecordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);
                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordMetadata = recordMetadata;

                        readAsyncResult = await session.ReadAtAddressAsync(readAtAddress, ref input, ReadFlags.SkipReadCache, default, maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordMetadata);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordMetadata.RecordInfo, recordMetadata.RecordInfo);
                    }
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public void ReadNoKeySyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            testStore.Populate(useRMW, useAsync: false).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var rng = new Random(101);
                var output = default(Value);
                var input = default(Value);

                for (int ii = 0; ii < numKeys; ++ii)
                {
                    var keyOrdinal = rng.Next(numKeys);
                    var status = session.ReadAtAddress(testStore.InsertAddresses[keyOrdinal], ref input, ref output, serialNo: maxLap + 1);
                    if (status == Status.PENDING)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = TestUtils.GetSinglePendingResult(completedOutputs);
                    }

                    TestStore.ProcessNoKeyRecord(status, ref output, keyOrdinal);
                }

                testStore.Flush().AsTask().GetAwaiter().GetResult();
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadNoKeyAsyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var rng = new Random(101);
                var input = default(Value);
                RecordMetadata recordMetadata = default;

                for (int ii = 0; ii < numKeys; ++ii)
                {
                    var keyOrdinal = rng.Next(numKeys);
                    var readAsyncResult = await session.ReadAtAddressAsync(testStore.InsertAddresses[keyOrdinal], ref input, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);
                    TestStore.ProcessNoKeyRecord(status, ref output, keyOrdinal);
                }
            }

            await testStore.Flush();
        }
    }

    [TestFixture]
    public class ReadMinAddressTests
    {
        const int numOps = 500;

        private IDevice log;
        private FasterKV<long, long> fht;
        private ClientSession<long, long, long, long, Empty, IFunctions<long, long, long, long, Empty>> session;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/SimpleRecoveryTest1.log", deleteOnClose: true);

            fht = new FasterKV<long, long>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 }
                );

            session = fht.NewSession(new SimpleFunctions<long, long>());
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV"), Category("Read")]
        public async ValueTask ReadMinAddressTest([Values] bool isAsync)
        {
            long minAddress = core.Constants.kInvalidAddress;
            var pivotKey = numOps / 2;
            long makeValue(long key) => key + numOps * 10;
            for (int ii = 0; ii < numOps; ii++)
            {
                if (ii == pivotKey)
                    minAddress = fht.Log.TailAddress;
                session.Upsert(ii, makeValue(ii));
            }

            // Verify the test set up correctly
            Assert.AreNotEqual(core.Constants.kInvalidAddress, minAddress);

            long input = 0;

            async ValueTask ReadMin(long key, Status expectedStatus)
            {
                Status status;
                long output = 0;
                if (isAsync)
                    (status, output) = (await session.ReadAsync(ref key, ref input, minAddress, ReadFlags.MinAddress)).Complete();
                else
                {
                    RecordMetadata recordMetadata = new(new RecordInfo { PreviousAddress = minAddress });
                    status = session.Read(ref key, ref input, ref output, ref recordMetadata, ReadFlags.MinAddress);
                    if (status == Status.PENDING)
                    {
                        Assert.IsTrue(session.CompletePendingWithOutputs(out var completedOutputs, wait: true));
                        (status, output) = TestUtils.GetSinglePendingResult(completedOutputs);
                    }
                }
                Assert.AreEqual(expectedStatus, status);
                if (status != Status.NOTFOUND)
                    Assert.AreEqual(output, makeValue(key));
            }

            async ValueTask RunTests()
            {
                // First read at the pivot, to verify that and make sure the rest of the test works
                await ReadMin(pivotKey, Status.OK);

                // Read a Key that is below the min address
                await ReadMin(pivotKey - 1, Status.NOTFOUND);

                // Read a Key that is above the min address
                await ReadMin(pivotKey + 1, Status.OK);
            }

            await RunTests();
            fht.Log.FlushAndEvict(wait: true);
            await RunTests();
        }
    }
}
