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

        public struct Output
        {
            public long value;
            public long address;

            public override string ToString() => $"val {value}; addr {address}";
        }

        private static long SetReadOutput(long key, long value) => (key << 32) | value;

        public enum UseReadCache { NoReadCache, ReadCache }

        internal class Functions : FunctionsBase<Key, Value, Value, Output, Empty>
        {
            internal long lastWriteAddress = Constants.kInvalidAddress;
            readonly bool useReadCache;
            internal ReadFlags readFlags = ReadFlags.DisableReadCache;

            internal Functions()
            {
                foreach (var arg in TestContext.CurrentContext.Test.Arguments)
                {
                    if (arg is UseReadCache urc)
                    {
                        this.useReadCache = urc == UseReadCache.ReadCache;
                        continue;
                    }
                }
            }

            public override bool ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Output output, ref ReadInfo readInfo)
            {
                output.value = SetReadOutput(key.key, value.value);
                output.address = readInfo.Address;
                return true;
            }

            public override bool SingleReader(ref Key key, ref Value input, ref Value value, ref Output output, ref ReadInfo readInfo)
            {
                output.value = SetReadOutput(key.key, value.value);
                output.address = readInfo.Address;
                return true;
            }

            // Return false to force a chain of values.
            public override bool ConcurrentWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo) => false;

            public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Output output, ref RMWInfo rmwInfo) => false;

            // Record addresses
            public override bool SingleWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                dst = src;
                this.lastWriteAddress = upsertInfo.Address;
                return true;
            }

            public override bool InitialUpdater(ref Key key, ref Value input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
            {
                this.lastWriteAddress = rmwInfo.Address;
                output.address = rmwInfo.Address;
                output.value = value.value = input.value;
                return true;
            }

            public override bool CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo)
            {
                this.lastWriteAddress = rmwInfo.Address;
                output.address = rmwInfo.Address;
                output.value = newValue.value = input.value;
                return true;
            }

            public override void ReadCompletionCallback(ref Key key, ref Value input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                if (status.Found)
                {
                    if (this.useReadCache && !this.readFlags.HasFlag(ReadFlags.DisableReadCacheReads))
                        Assert.AreEqual(Constants.kInvalidAddress, recordMetadata.Address, $"key {key}");
                    else
                        Assert.AreEqual(output.address, recordMetadata.Address, $"key {key}");
                }
            }

            public override void RMWCompletionCallback(ref Key key, ref Value input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                if (status.Found)
                    Assert.AreEqual(output.address, recordMetadata.Address);
            }
        }

        private class TestStore : IDisposable
        {
            internal FasterKV<Key, Value> fkv;
            internal IDevice logDevice;
            internal string testDir;
            private readonly bool flush;

            internal long[] InsertAddresses = new long[numKeys];

            internal TestStore(bool useReadCache, ReadFlags readFlags, bool flush)
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
                    ReadFlags = readFlags,
                    // Use small-footprint values
                    PageSizeBits = 12, // (4K pages)
                    MemorySizeBits = 20 // (1M memory for main log)
                };

                this.fkv = new FasterKV<Key, Value>(
                    size: 1L << 20,
                    logSettings: logSettings,
                    checkpointSettings: new CheckpointSettings { CheckpointDir = $"{this.testDir}/CheckpointDir" },
                    serializerSettings: null,
                    comparer: new Key.Comparer(),
                    disableLocking: true
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

                    if (status.IsPending)
                        await session.CompletePendingAsync();

                    InsertAddresses[ii] = functions.lastWriteAddress;
                    //Assert.IsTrue(session.ctx.HasNoPendingRequests);

                    // Illustrate that deleted records can be shown as well (unless overwritten by in-place operations, which are not done here)
                    if (lap == deleteLap)
                        session.Delete(ref key, serialNo: lap);
                }

                await Flush();
            }

            internal bool ProcessChainRecord(Status status, RecordMetadata recordMetadata, int lap, ref Output actualOutput)
            {
                var recordInfo = recordMetadata.RecordInfo;
                Assert.GreaterOrEqual(lap, 0);
                long expectedValue = SetReadOutput(defaultKeyToScan, LapOffset(lap) + defaultKeyToScan);

                Assert.AreEqual(status.NotFound, recordInfo.Tombstone, $"status({status}) == NOTFOUND != Tombstone ({recordInfo.Tombstone}) on lap {lap}");
                Assert.AreEqual(lap == deleteLap, recordInfo.Tombstone, $"lap({lap}) == deleteLap({deleteLap}) != Tombstone ({recordInfo.Tombstone})");
                if (!recordInfo.Tombstone)
                    Assert.AreEqual(expectedValue, actualOutput.value, $"lap({lap})");

                // Check for end of loop
                return recordInfo.PreviousAddress >= fkv.Log.BeginAddress;
            }

            internal static void ProcessNoKeyRecord(Status status, ref Output actualOutput, int keyOrdinal)
            {
                if (status.Found)
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
        [TestCase(UseReadCache.NoReadCache, ReadFlags.None, false, false)]
        [TestCase(UseReadCache.NoReadCache, ReadFlags.CopyReadsToTail | ReadFlags.CopyFromDeviceOnly, true, true)]
        [TestCase(UseReadCache.ReadCache, ReadFlags.None, false, true)]
        [Category("FasterKV")]
        public void VersionedReadSyncTests(UseReadCache urc, ReadFlags readFlags, bool useRMW, bool flush)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            using var testStore = new TestStore(useReadCache, readFlags, flush);
            testStore.Populate(useRMW, useAsync:false).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Output);
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { ReadFlags = session.functions.readFlags };

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var status = session.Read(ref key, ref input, ref output, ref readOptions, out _, serialNo: maxLap + 1);

                    if (status.IsPending)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = TestUtils.GetSinglePendingResult(completedOutputs, out recordMetadata);
                    }
                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readOptions.StartAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadFlags.None, false, false)]
        [TestCase(UseReadCache.NoReadCache, ReadFlags.CopyReadsToTail | ReadFlags.CopyFromDeviceOnly, true, true)]
        [TestCase(UseReadCache.ReadCache, ReadFlags.None, false, true)]
        [Category("FasterKV")]
        public async Task VersionedReadAsyncTests(UseReadCache urc, ReadFlags readFlags, bool useRMW, bool flush)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            using var testStore = new TestStore(useReadCache, readFlags, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { ReadFlags = session.functions.readFlags };

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAsyncResult = await session.ReadAsync(ref key, ref input, ref readOptions, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;
                    readOptions.StartAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadFlags.None, false, false)]
        [TestCase(UseReadCache.NoReadCache, ReadFlags.CopyReadsToTail | ReadFlags.CopyFromDeviceOnly, true, true)]
        [TestCase(UseReadCache.ReadCache, ReadFlags.None, false, true)]
        [Category("FasterKV")]
        public void ReadAtAddressSyncTests(UseReadCache urc, ReadFlags readFlags, bool useRMW, bool flush)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            using var testStore = new TestStore(useReadCache, readFlags, flush);
            testStore.Populate(useRMW, useAsync: false).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Output);
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { ReadFlags = session.functions.readFlags };

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var status = session.Read(ref key, ref input, ref output, ref readOptions, out recordMetadata, serialNo: maxLap + 1);
                    if (status.IsPending)
                    {
                        // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, output) = TestUtils.GetSinglePendingResult(completedOutputs, out recordMetadata);
                    }

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;

                    if (readOptions.StartAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordMetadata = recordMetadata;

                        status = session.ReadAtAddress(ref input, ref output, ref readOptions, serialNo: maxLap + 1);
                        if (status.IsPending)
                        {
                            // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            (status, output) = TestUtils.GetSinglePendingResult(completedOutputs, out recordMetadata);
                        }

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordMetadata.RecordInfo, recordMetadata.RecordInfo);
                    }
                    readOptions.StartAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadFlags.None, false, false)]
        [TestCase(UseReadCache.NoReadCache, ReadFlags.CopyReadsToTail | ReadFlags.CopyFromDeviceOnly, true, true)]
        [TestCase(UseReadCache.ReadCache, ReadFlags.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadAtAddressAsyncTests(UseReadCache urc, ReadFlags readFlags, bool useRMW, bool flush)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            using var testStore = new TestStore(useReadCache, readFlags, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { ReadFlags = session.functions.readFlags };

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAsyncResult = await session.ReadAsync(ref key, ref input, ref readOptions, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;

                    if (readOptions.StartAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordMetadata = recordMetadata;

                        readAsyncResult = await session.ReadAtAddressAsync(ref input, ref readOptions, default, serialNo: maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordMetadata);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordMetadata.RecordInfo, recordMetadata.RecordInfo);
                    }

                    readOptions.StartAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // Test is similar to others but tests the Overload where RadFlag.none is set -- probably don't need all combinations of test but doesn't hurt 
        [TestCase(UseReadCache.NoReadCache, ReadFlags.None, false, false)]
        [TestCase(UseReadCache.NoReadCache, ReadFlags.CopyReadsToTail | ReadFlags.CopyFromDeviceOnly, true, true)]
        [TestCase(UseReadCache.ReadCache, ReadFlags.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadAtAddressAsyncReadFlagsNoneTests(UseReadCache urc, ReadFlags readFlags, bool useRMW, bool flush)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            using var testStore = new TestStore(useReadCache, readFlags, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = new() { ReadFlags = session.functions.readFlags };

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAsyncResult = await session.ReadAsync(ref key, ref input, ref readOptions, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;

                    if (readOptions.StartAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordMetadata = recordMetadata;

                        readAsyncResult = await session.ReadAtAddressAsync(ref input, ref readOptions, default, serialNo: maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordMetadata);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordMetadata.RecordInfo, recordMetadata.RecordInfo);
                    }

                    readOptions.StartAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // Test is similar to others but tests the Overload where ReadFlag.SkipReadCache is set
        [TestCase(UseReadCache.NoReadCache, ReadFlags.None, false, false)]
        [TestCase(UseReadCache.NoReadCache, ReadFlags.CopyReadsToTail | ReadFlags.CopyFromDeviceOnly, true, true)]
        [TestCase(UseReadCache.ReadCache, ReadFlags.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadAtAddressAsyncReadFlagsSkipCacheTests(UseReadCache urc, ReadFlags readFlags, bool useRMW, bool flush)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            using var testStore = new TestStore(useReadCache, readFlags, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordMetadata recordMetadata = default;
                ReadOptions readOptions = default;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    readOptions.ReadFlags = session.functions.readFlags;
                    var readAsyncResult = await session.ReadAsync(ref key, ref input, ref readOptions, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordMetadata);

                    if (!testStore.ProcessChainRecord(status, recordMetadata, lap, ref output))
                        break;

                    if (readOptions.StartAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordMetadata = recordMetadata;

                        readOptions.ReadFlags = ReadFlags.DisableReadCacheReads | ReadFlags.DisableReadCacheUpdates;
                        readAsyncResult = await session.ReadAtAddressAsync(ref input, ref readOptions, default, maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordMetadata);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordMetadata.RecordInfo, recordMetadata.RecordInfo);
                    }
                    readOptions.StartAddress = recordMetadata.RecordInfo.PreviousAddress;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(UseReadCache.NoReadCache, ReadFlags.None, false, false)]
        [TestCase(UseReadCache.NoReadCache, ReadFlags.CopyReadsToTail | ReadFlags.CopyFromDeviceOnly, true, true)]
        [TestCase(UseReadCache.ReadCache, ReadFlags.None, false, true)]
        [Category("FasterKV")]
        public void ReadNoKeySyncTests(UseReadCache urc, ReadFlags readFlags, bool useRMW, bool flush)        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            using var testStore = new TestStore(useReadCache, readFlags, flush);
            testStore.Populate(useRMW, useAsync: false).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var rng = new Random(101);
                var output = default(Output);
                var input = default(Value);

                for (int ii = 0; ii < numKeys; ++ii)
                {
                    var keyOrdinal = rng.Next(numKeys);

                    ReadOptions readOptions = new()
                    {
                        StartAddress = testStore.InsertAddresses[keyOrdinal],
                        ReadFlags = session.functions.readFlags
                    };
                    var status = session.ReadAtAddress(ref input, ref output, ref readOptions, serialNo: maxLap + 1);
                    if (status.IsPending)
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
        [TestCase(UseReadCache.NoReadCache, ReadFlags.None, false, false)]
        [TestCase(UseReadCache.NoReadCache, ReadFlags.CopyReadsToTail | ReadFlags.CopyFromDeviceOnly, true, true)]
        [TestCase(UseReadCache.ReadCache, ReadFlags.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadNoKeyAsyncTests(UseReadCache urc, ReadFlags readFlags, bool useRMW, bool flush)
        {
            var useReadCache = urc == UseReadCache.ReadCache;
            using var testStore = new TestStore(useReadCache, readFlags, flush);
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

                    ReadOptions readOptions = new()
                    {
                        StartAddress = testStore.InsertAddresses[keyOrdinal],
                        ReadFlags = session.functions.readFlags
                    };

                    var readAsyncResult = await session.ReadAtAddressAsync(ref input, ref readOptions, default, serialNo: maxLap + 1);
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
                ReadOptions readOptions = new() { StopAddress = minAddress };
                if (isAsync)
                    (status, output) = (await session.ReadAsync(ref key, ref input, ref readOptions)).Complete();
                else
                {
                    status = session.Read(ref key, ref input, ref output, ref readOptions, out _);
                    if (status.IsPending)
                    {
                        Assert.IsTrue(session.CompletePendingWithOutputs(out var completedOutputs, wait: true));
                        (status, output) = TestUtils.GetSinglePendingResult(completedOutputs);
                    }
                }
                Assert.AreEqual(expectedStatus, status);
                if (status.Found)
                    Assert.AreEqual(output, makeValue(key));
            }

            async ValueTask RunTests()
            {
                // First read at the pivot, to verify that and make sure the rest of the test works
                await ReadMin(pivotKey, new(StatusCode.Found));

                // Read a Key that is below the min address
                await ReadMin(pivotKey - 1, new(StatusCode.NotFound));

                // Read a Key that is above the min address
                await ReadMin(pivotKey + 1, new(StatusCode.Found));
            }

            await RunTests();
            fht.Log.FlushAndEvict(wait: true);
            await RunTests();
        }
    }
}
