// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using FASTER.core;
using NUnit.Framework;
using System.Threading.Tasks;

namespace FASTER.test.recovery.sumstore
{
    [TestFixture]
    internal class DeviceTypeRecoveryTests
    {
        internal const long numUniqueKeys = (1 << 12);
        internal const long keySpace = (1L << 14);
        internal const long numOps = (1L << 17);
        internal const long completePendingInterval = (1L << 10);
        internal const long checkpointInterval = (1L << 14);

        private FasterKV<AdId, NumClicks> fht;
        private string path;
        private readonly List<Guid> logTokens = new();
        private readonly List<Guid> indexTokens = new();
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Only clean these in the initial Setup, as tests use the other Setup() overload to recover
            logTokens.Clear();
            indexTokens.Clear();
            TestUtils.DeleteDirectory(path, true);
        }

        private void Setup(TestUtils.DeviceType deviceType)
        {
            log = TestUtils.CreateTestDevice(deviceType, path + "Test.log");
            fht = new FasterKV<AdId, NumClicks>(keySpace,
                //new LogSettings { LogDevice = log, MemorySizeBits = 14, PageSizeBits = 9 },  // locks ups at session.RMW line in Populate() for Local Memory
                new LogSettings { LogDevice = log, SegmentSizeBits = 25 },
                new CheckpointSettings { CheckpointDir = path }
            );
        }

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        private void TearDown(bool deleteDir)
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;

            // Do NOT clean up here unless specified, as tests use this TearDown() to prepare for recovery
            if (deleteDir)
                TestUtils.DeleteDirectory(path);
        }

        private void PrepareToRecover(TestUtils.DeviceType deviceType)
        {
            TearDown(deleteDir: false);
            Setup(deviceType);
        }

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestSeparateCheckpoint([Values]bool isAsync, [Values] TestUtils.DeviceType deviceType)
        {
            Setup(deviceType);
            Populate(SeparateCheckpointAction);

            for (var i = 0; i < logTokens.Count; i++)
            {
                if (i >= indexTokens.Count) break;
                PrepareToRecover(deviceType);
                await RecoverAndTestAsync(i, isAsync);
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async ValueTask RecoveryTestFullCheckpoint([Values] bool isAsync, [Values] TestUtils.DeviceType deviceType)
        {
            Setup(deviceType);
            Populate(FullCheckpointAction);

            for (var i = 0; i < logTokens.Count; i++)
            {
                PrepareToRecover(deviceType);
                await RecoverAndTestAsync(i, isAsync);
            }
        }

        private void FullCheckpointAction(int opNum)
        {
            if ((opNum + 1) % checkpointInterval == 0)
            {
                Guid token;
                while (!fht.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot)) { }
                logTokens.Add(token);
                indexTokens.Add(token);
                fht.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
        }

        private void SeparateCheckpointAction(int opNum) 
        {
            if ((opNum + 1) % checkpointInterval != 0) 
                return;

            var checkpointNum = (opNum + 1) / checkpointInterval;
            Guid token;
            if (checkpointNum % 2 == 1)
            {
                while (!fht.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot)) { }
                logTokens.Add(token);
            }
            else
            {
                while (!fht.TryInitiateIndexCheckpoint(out token)) { }
                indexTokens.Add(token);
            }
            fht.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
        }

        private void Populate(Action<int> checkpointAction)
        {
            // Prepare the dataset
            var inputArray = new AdInput[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId.adId = i % numUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            // Register thread with FASTER
            using var session = fht.NewSession(new Functions());

            // Process the batch of input data
            for (int i = 0; i < numOps; i++)
            {
                session.RMW(ref inputArray[i].adId, ref inputArray[i], Empty.Default, i);

                checkpointAction(i);

                if (i % completePendingInterval == 0)
                    session.CompletePending(false);
            }

            // Make sure operations are completed
            session.CompletePending(true);
        }

        private async ValueTask RecoverAndTestAsync(int tokenIndex, bool isAsync)
        {
            var logToken = logTokens[tokenIndex];
            var indexToken = indexTokens[tokenIndex];

            // Recover
            if (isAsync)
                await fht.RecoverAsync(indexToken, logToken);
            else
                fht.Recover(indexToken, logToken);

            // Create array for reading
            var inputArray = new AdInput[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                inputArray[i].adId.adId = i;
                inputArray[i].numClicks.numClicks = 0;
            }

            // Register with thread
            var session = fht.NewSession(new Functions());

            AdInput input = default;
            Output output = default;

            // Issue read requests
            for (var i = 0; i < numUniqueKeys; i++)
            {
                var status = session.Read(ref inputArray[i].adId, ref input, ref output, Empty.Default, i);
                Assert.AreEqual(Status.OK, status, $"At tokenIndex {tokenIndex}, keyIndex {i}, AdId {inputArray[i].adId.adId}");
                inputArray[i].numClicks = output.value;
            }

            // Complete all pending requests
            session.CompletePending(true);

            // Release
            session.Dispose();

            // Test outputs
            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(logToken,
                new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactory(),
                        new DefaultCheckpointNamingScheme(
                          new DirectoryInfo(path).FullName)));

            // Compute expected array
            long[] expected = new long[numUniqueKeys];
            foreach (var guid in checkpointInfo.continueTokens.Keys)
            {
                var cp = checkpointInfo.continueTokens[guid];
                for (long i = 0; i <= cp.UntilSerialNo; i++)
                {
                    var id = i % numUniqueKeys;
                    expected[id]++;
                }
            }

            int threadCount = 1; // single threaded test
            int numCompleted = threadCount - checkpointInfo.continueTokens.Count;
            for (int t = 0; t < numCompleted; t++)
            {
                var sno = numOps;
                for (long i = 0; i < sno; i++)
                {
                    var id = i % numUniqueKeys;
                    expected[id]++;
                }
            }

            // Assert if expected is same as found
            for (long i = 0; i < numUniqueKeys; i++)
            {
                Assert.AreEqual(expected[i], inputArray[i].numClicks.numClicks, $"At keyIndex {i}, AdId {inputArray[i].adId.adId}");
            }
        }
    }

    [TestFixture]
    internal class AllocatorTypeRecoveryTests
    {
        // VarLenMax is the variable-length portion; 2 is for the fixed fields
        const int VarLenMax = 10;
        const int StackAllocMax = VarLenMax + 2;
        const int RandSeed = 101;
        const long expectedValueBase = DeviceTypeRecoveryTests.numUniqueKeys * (DeviceTypeRecoveryTests.numOps / DeviceTypeRecoveryTests.numUniqueKeys - 1);
        private static long ExpectedValue(int key) => expectedValueBase + key;

        private IDisposable fhtDisp;
        private string path;
        private Guid logToken;
        private Guid indexToken;
        private IDevice log;
        private IDevice objlog;
        private bool smallSector;

        // 'object' to avoid generic args
        private object varLenStructObj;
        private object serializerSettingsObj;

        [SetUp]
        public void Setup()
        {
            smallSector = false;
            varLenStructObj = null;
            serializerSettingsObj = null;

            path = TestUtils.MethodTestDir + "/";

            // Only clean these in the initial Setup, as tests use the other Setup() overload to recover
            logToken = Guid.Empty;
            indexToken = Guid.Empty;
            TestUtils.DeleteDirectory(path, true);
        }

        private FasterKV<TData, TData> Setup<TData>()
        {
            log = new LocalMemoryDevice(1L << 26, 1L << 22, 2, sector_size: smallSector ? 64 : (uint)512, fileName: $"{path}{typeof(TData).Name}.log");
            objlog = serializerSettingsObj is null
                ? null
                : new LocalMemoryDevice(1L << 26, 1L << 22, 2, fileName: $"{path}{typeof(TData).Name}.obj.log");

            var varLenStruct = this.varLenStructObj as IVariableLengthStruct<TData>;
            Assert.AreEqual(this.varLenStructObj is null, varLenStruct is null, "varLenStructSettings");
            VariableLengthStructSettings<TData, TData> varLenStructSettings = varLenStruct is null
                ? null
                : new VariableLengthStructSettings<TData, TData> { keyLength = varLenStruct, valueLength = varLenStruct };

            var result = new FasterKV<TData, TData>(DeviceTypeRecoveryTests.keySpace,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, SegmentSizeBits = 25 },
                new CheckpointSettings { CheckpointDir = path },
                this.serializerSettingsObj as SerializerSettings<TData, TData>,
                variableLengthStructSettings: varLenStructSettings
            );

            fhtDisp = result;
            return result;
        }

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        private void TearDown(bool deleteDir)
        {
            fhtDisp?.Dispose();
            fhtDisp = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            // Do NOT clean up here unless specified, as tests use this TearDown() to prepare for recovery
            if (deleteDir)
                TestUtils.DeleteDirectory(path);
        }

        private FasterKV<TData, TData> PrepareToRecover<TData>()
        {
            TearDown(deleteDir: false);
            return Setup<TData>();
        }

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestByAllocatorType([Values] TestUtils.AllocatorType allocatorType, [Values] bool isAsync)
        {
            await TestDriver(allocatorType, isAsync);
        }

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestFailOnSectorSize([Values] TestUtils.AllocatorType allocatorType, [Values] bool isAsync)
        {
            this.smallSector = true;
            await TestDriver(allocatorType, isAsync);
        }

        private async ValueTask TestDriver(TestUtils.AllocatorType allocatorType, [Values] bool isAsync)
        { 
            ValueTask task;
            switch (allocatorType)
            {
                case TestUtils.AllocatorType.FixedBlittable:
                    task = RunTest<long>(Populate, Read, Recover, isAsync);
                    break;
                case TestUtils.AllocatorType.VarLenBlittable:
                    this.varLenStructObj = new VLValue();
                    task = RunTest<VLValue>(Populate, Read, Recover, isAsync);
                    break;
                case TestUtils.AllocatorType.Generic:
                    this.serializerSettingsObj = new MyValueSerializer();
                    task = RunTest<MyValue>(Populate, Read, Recover, isAsync);
                    break;
                default:
                    throw new ApplicationException("Unknown allocator type");
            };
            await task;
        }

        private async ValueTask RunTest<TData>(Action<FasterKV<TData, TData>> populateAction, Action<FasterKV<TData, TData>> readAction, Func<FasterKV<TData, TData>, bool, ValueTask> recoverFunc, bool isAsync)
        {
            var fht = Setup<TData>();
            populateAction(fht);
            readAction(fht);
            if (smallSector)
            {
                Assert.ThrowsAsync<FasterException>(async () => await Checkpoint(fht, isAsync));
                Assert.Pass("Verified expected exception; the test cannot continue, so exiting early with success");
            }
            else
                await Checkpoint(fht, isAsync);

            Assert.AreNotEqual(Guid.Empty, this.logToken);
            Assert.AreNotEqual(Guid.Empty, this.indexToken);
            readAction(fht);

            fht = PrepareToRecover<TData>();
            await recoverFunc(fht, isAsync);
            readAction(fht);
        }

        private void Populate(FasterKV<long, long> fht)
        {
            using var session = fht.NewSession(new SimpleFunctions<long, long>());

            for (int i = 0; i < DeviceTypeRecoveryTests.numOps; i++)
                session.Upsert(i % DeviceTypeRecoveryTests.numUniqueKeys, i);
            session.CompletePending(true);
        }

        static int GetVarLen(Random r) => r.Next(VarLenMax) + 2;

        private unsafe void Populate(FasterKV<VLValue, VLValue> fht)
        {
            using var session = fht.NewSession(new VLFunctions2());
            Random rng = new(RandSeed);

            // Single alloc outside the loop, to the max length we'll need.
            int* keyval = stackalloc int[StackAllocMax];
            int* val = stackalloc int[StackAllocMax];

            for (int i = 0; i < DeviceTypeRecoveryTests.numOps; i++)
            {
                // We must be consistent on length across iterations of each key value
                var key0 = i % (int)DeviceTypeRecoveryTests.numUniqueKeys;
                if (key0 == 0)
                    rng = new (RandSeed);

                ref VLValue key1 = ref *(VLValue*)keyval;
                key1.length = 2;
                key1.field1 = key0;

                var len = GetVarLen(rng);
                ref VLValue value = ref *(VLValue*)val;
                value.length = len;
                for (int j = 1; j < len; j++)
                    *(val + j) = i;

                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }
            session.CompletePending(true);
        }

        private unsafe void Populate(FasterKV<MyValue, MyValue> fht)
        {
            using var session = fht.NewSession(new MyFunctions2());

            for (int i = 0; i < DeviceTypeRecoveryTests.numOps; i++)
            {
                var key = new MyValue { value = i % (int)DeviceTypeRecoveryTests.numUniqueKeys };
                var value = new MyValue { value = i };
                session.Upsert(key, value);
            }
            session.CompletePending(true);
        }

        private async ValueTask Checkpoint<TData>(FasterKV<TData, TData> fht, bool isAsync)
        {
            if (isAsync)
            {
                var (success, token) = await fht.TakeFullCheckpointAsync(CheckpointType.Snapshot);
                Assert.IsTrue(success);
                this.logToken = token;
            }
            else
            {
                while (!fht.TryInitiateFullCheckpoint(out this.logToken, CheckpointType.Snapshot)) { }
                fht.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            this.indexToken = this.logToken;
        }

        private async ValueTask RecoverAndReadTest(FasterKV<long, long> fht, bool isAsync)
        {
            await Recover(fht, isAsync);
            Read(fht);
        }

        private static void Read(FasterKV<long, long> fht)
        {
            using var session = fht.NewSession(new SimpleFunctions<long, long>());

            for (var i = 0; i < DeviceTypeRecoveryTests.numUniqueKeys; i++)
            {
                var status = session.Read(i % DeviceTypeRecoveryTests.numUniqueKeys, default, out long output);
                Assert.AreEqual(Status.OK, status, $"keyIndex {i}");
                Assert.AreEqual(ExpectedValue(i), output);
            }
        }

        private async ValueTask RecoverAndReadTest(FasterKV<VLValue, VLValue> fht, bool isAsync)
        {
            await Recover(fht, isAsync);
            Read(fht);
        }

        private static void Read(FasterKV<VLValue, VLValue> fht)
        {
            using var session = fht.NewSession(new VLFunctions2());

            Random rng = new (RandSeed);
            Input input = default;

            for (var i = 0; i < DeviceTypeRecoveryTests.numUniqueKeys; i++)
            {
                var key1 = new VLValue { length = 2, field1 = i };
                var len = GetVarLen(rng);

                int[] output = null;
                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(len, output[0], "Length");
                Assert.AreEqual(ExpectedValue(i), output[1], "field1");
                for (int j = 2; j < len; j++)
                    Assert.AreEqual(ExpectedValue(i), output[j], "extra data at position {j}");
            }
        }

        private async ValueTask RecoverAndReadTest(FasterKV<MyValue, MyValue> fht, bool isAsync)
        {
            await Recover(fht, isAsync);
            Read(fht);
        }

        private static void Read(FasterKV<MyValue, MyValue> fht)
        {
            using var session = fht.NewSession(new MyFunctions2());

            for (var i = 0; i < DeviceTypeRecoveryTests.numUniqueKeys; i++)
            {
                var key = new MyValue { value = i };
                var status = session.Read(key, default, out MyOutput output);
                Assert.AreEqual(Status.OK, status, $"keyIndex {i}");
                Assert.AreEqual(ExpectedValue(i), output.value.value);
            }
        }

        private async ValueTask Recover<TData>(FasterKV<TData, TData> fht, bool isAsync = false)
        {
            if (isAsync)
                await fht.RecoverAsync(this.indexToken, this.logToken);
            else
                fht.Recover(this.indexToken, this.logToken);
        }
    }
}
