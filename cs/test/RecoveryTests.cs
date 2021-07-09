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

        private List<Guid> logTokens, indexTokens;
        private IDevice log;

        [SetUp]
        public void Setup() => Setup(deleteDir:true);

        public void Setup(bool deleteDir)
        {
            path = TestUtils.MethodTestDir + "/";

            // Do NOT clean up here unless specified, as tests use this Setup() to recover
            if (deleteDir)
                TestUtils.DeleteDirectory(path, true);

            log = Devices.CreateLogDevice(path + "FullRecoveryTests.log");

            fht = new FasterKV<AdId, NumClicks>
            (keySpace,
                new LogSettings {LogDevice = log},
                new CheckpointSettings {CheckpointDir = path, CheckPointType = CheckpointType.Snapshot}
            );
        }

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        public void TearDown(bool deleteDir)
        {
            //** #142980 - Blob not exist exception in Dispose so use Try \ Catch to make sure tests run without issues 
            try
            {
                fht?.Dispose();
                fht = null;
                log?.Dispose();
                log = null;
            }
            catch { }

            // Do NOT clean up here unless specified, as tests use this Setup() to recover
            if (deleteDir)
                TestUtils.DeleteDirectory(path);
        }

        private void PrepareToRecover()
        {
            TearDown(deleteDir: false);
            Setup(deleteDir: false);
        }

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestSeparateCheckpoint([Values]bool isAsync)
        {
            Populate(SeparateCheckpointAction);

            for (var i = 0; i < logTokens.Count; i++)
            {
                if (i >= indexTokens.Count) break;
                PrepareToRecover();
                await RecoverAndTestAsync(logTokens[i], indexTokens[i], isAsync);
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async ValueTask RecoveryTestFullCheckpoint([Values] bool isAsync, [Values] TestUtils.DeviceType deviceType)
        {
            // Reset all the log and fht values since using all deviceType
            log = TestUtils.CreateTestDevice(deviceType, path + "FullRecoveryTests.log");
            fht = new FasterKV<AdId, NumClicks>
            (keySpace,
                //new LogSettings { LogDevice = log, MemorySizeBits = 14, PageSizeBits = 9 },  // locks ups at session.RMW line in Populate() for Local Memory
                new LogSettings { LogDevice = log, SegmentSizeBits = 25 },
                new CheckpointSettings { CheckpointDir = path, CheckPointType = CheckpointType.Snapshot }
            );


            //*** Bug #143433 - RecoveryTestFullCheckpoint - LocalMemory only - only reading 1 item when other devices reading all 16384 items
            if (deviceType == TestUtils.DeviceType.LocalMemory)
            {
                return;
            }
            //*** Bug #143433 - RecoveryTestFullCheckpoint - LocalMemory only - only reading 1 item when other devices reading all 16384 items



            Populate(FullCheckpointAction);

            foreach (var token in logTokens)
            {
                PrepareToRecover();
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
                Assert.IsTrue(status == Status.OK,"Expected status=OK but found status:"+status.ToString()+ " at index:" + i.ToString());
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
                Assert.IsTrue(
                    expected[i] == inputArray[i].numClicks.numClicks,
                    "Debug error for AdId {0}: Expected ({1}), Found({2})", inputArray[i].adId.adId, expected[i],
                    inputArray[i].numClicks.numClicks);
            }
        }
    }
}