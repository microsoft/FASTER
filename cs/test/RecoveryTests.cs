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
    internal class RecoveryTests
    {
        const long numUniqueKeys = (1 << 14);
        const long keySpace = (1L << 14);
        const long numOps = (1L << 19);
        const long completePendingInterval = (1L << 10);
        const long checkpointInterval = (1L << 16);

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
            fht = new FasterKV<AdId, NumClicks>
            (keySpace,
                //new LogSettings { LogDevice = log, MemorySizeBits = 14, PageSizeBits = 9 },  // locks ups at session.RMW line in Populate() for Local Memory
                new LogSettings { LogDevice = log, SegmentSizeBits = 25 },
                new CheckpointSettings { CheckpointDir = path, CheckPointType = CheckpointType.Snapshot }
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
            {
                logTokens.Clear();
                indexTokens.Clear();
                TestUtils.DeleteDirectory(path);
            }
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

        public void FullCheckpointAction(int opNum)
        {
            if ((opNum + 1) % checkpointInterval == 0)
            {
                Guid token;
                while (!fht.TakeFullCheckpoint(out token))
                {
                }

                logTokens.Add(token);
                indexTokens.Add(token);

                fht.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
        }

        public void SeparateCheckpointAction(int opNum) 
        {
            if ((opNum + 1) % checkpointInterval != 0) return;

            var checkpointNum = (opNum + 1) / checkpointInterval;
            if (checkpointNum % 2 == 1)
            {
                Guid token;
                while (!fht.TakeHybridLogCheckpoint(out token))
                {
                }

                logTokens.Add(token);
                fht.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            else
            {
                Guid token;
                while (!fht.TakeIndexCheckpoint(out token))
                {
                }

                indexTokens.Add(token);
                fht.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
        }

        public void Populate(Action<int> checkpointAction)
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
                {
                    session.CompletePending(false);
                }
            }

            // Make sure operations are completed
            session.CompletePending(true);
        }

        public async ValueTask RecoverAndTestAsync(int tokenIndex, bool isAsync)
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
}