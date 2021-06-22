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
        private string test_path;
        private List<Guid> logTokens, indexTokens;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            test_path = TestUtils.MethodTestDir;
            if (!Directory.Exists(test_path))
                Directory.CreateDirectory(test_path);

            log = Devices.CreateLogDevice(test_path + "/FullRecoveryTests.log");

            fht = new FasterKV<AdId, NumClicks>
            (keySpace,
                new LogSettings {LogDevice = log},
                new CheckpointSettings {CheckpointDir = test_path, CheckPointType = CheckpointType.Snapshot}
            );
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(test_path);
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask RecoveryTestSeparateCheckpoint([Values]bool isAsync)
        {
            Populate(SeparateCheckpointAction);

            for (var i = 0; i < logTokens.Count; i++)
            {
                if (i >= indexTokens.Count) break;
                fht.Dispose();
                fht = null;
                log.Dispose();
                log = null;
                Setup();
                await RecoverAndTestAsync(logTokens[i], indexTokens[i], isAsync);
            }
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask RecoveryTestFullCheckpoint([Values] bool isAsync)
        {
            Populate(FullCheckpointAction);

            foreach (var token in logTokens)
            {
                fht.Dispose();
                fht = null;
                log.Dispose();
                log = null;
                Setup();
                await RecoverAndTestAsync(token, token, isAsync);
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

                fht.CompleteCheckpointAsync().GetAwaiter().GetResult();
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
                fht.CompleteCheckpointAsync().GetAwaiter().GetResult();
            }
            else
            {
                Guid token;
                while (!fht.TakeIndexCheckpoint(out token))
                {
                }

                indexTokens.Add(token);
                fht.CompleteCheckpointAsync().GetAwaiter().GetResult();
            }
        }

        public void Populate(Action<int> checkpointAction)
        {
            logTokens = new List<Guid>();
            indexTokens = new List<Guid>();
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

            // Deregister thread from FASTER
            session.Dispose();
        }

        public async ValueTask RecoverAndTestAsync(Guid cprVersion, Guid indexVersion, bool isAsync)
        {
            // Recover
            if (isAsync)
                await fht.RecoverAsync(indexVersion, cprVersion);
            else
                fht.Recover(indexVersion, cprVersion);

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
                Assert.IsTrue(status == Status.OK);
                inputArray[i].numClicks = output.value;
            }

            // Complete all pending requests
            session.CompletePending(true);

            // Release
            session.Dispose();

            // Test outputs
            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(cprVersion,
                new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactory(),
                        new DefaultCheckpointNamingScheme(
                          new DirectoryInfo(test_path).FullName)));

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
                Assert.IsTrue(
                    expected[i] == inputArray[i].numClicks.numClicks,
                    "Debug error for AdId {0}: Expected ({1}), Found({2})", inputArray[i].adId.adId, expected[i],
                    inputArray[i].numClicks.numClicks);
            }
        }
    }
}