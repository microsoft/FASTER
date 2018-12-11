// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.recovery.sumstore
{

    [TestFixture]
    internal class FullRecoveryTests
    {
        const long numUniqueKeys = (1 << 14);
        const long keySpace = (1L << 14);
        const long numOps = (1L << 19);
        const long refreshInterval = (1L << 8);
        const long completePendingInterval = (1L << 10);
        const long checkpointInterval = (1L << 16);
        private FasterKV<AdId, NumClicks, Input, Output, Empty, Functions> fht;
        private string test_path;
        private Guid token;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            if (test_path == null)
            {
                test_path = Path.GetTempPath() + Path.GetRandomFileName();
                if (!Directory.Exists(test_path))
                    Directory.CreateDirectory(test_path);
            }

            log = FasterFactory.CreateLogDevice(test_path + "\\hlog");

            fht = new FasterKV<AdId, NumClicks, Input, Output, Empty, Functions>
                (keySpace, new Functions(), 
                new LogSettings { LogDevice = log },
                new CheckpointSettings { CheckpointDir = test_path, CheckPointType = CheckpointType.Snapshot }
                );
        }

        [TearDown]
        public void TearDown()
        {
            fht.StopSession();
            fht.Dispose();
            fht = null;
            log.Close();
            DeleteDirectory(test_path);
        }

        public static void DeleteDirectory(string path)
        {
            foreach (string directory in Directory.GetDirectories(path))
            {
                DeleteDirectory(directory);
            }

            try
            {
                Directory.Delete(path, true);
            }
            catch (IOException)
            {
                Directory.Delete(path, true);
            }
            catch (UnauthorizedAccessException)
            {
                Directory.Delete(path, true);
            }
        }

        [Test]
        public void RecoveryTest1()
        {
            Populate();
            log.Close();
            Setup();
            RecoverAndTest(token, token);
        }

        public void Populate()
        {
            Empty context;

            // Prepare the dataset
            var inputArray = new Input[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId.adId = i % numUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            // Register thread with FASTER
            fht.StartSession();

            // Prpcess the batch of input data
            bool first = true;
            for (int i = 0; i < numOps; i++)
            {
                fht.RMW(ref inputArray[i].adId, ref inputArray[i], ref context, i);

                if ((i+1) % checkpointInterval == 0)
                {
                    if (first)
                        while (!fht.TakeFullCheckpoint(out token))
                            fht.Refresh();
                    else
                        while (!fht.TakeFullCheckpoint(out Guid nextToken))
                            fht.Refresh();

                    fht.CompleteCheckpoint(true);

                    first = false;
                }

                if (i % completePendingInterval == 0)
                {
                    fht.CompletePending(false);
                }
                else if (i % refreshInterval == 0)
                {
                    fht.Refresh();
                }
            }

            // Make sure operations are completed
            fht.CompletePending(true);

            // Deregister thread from FASTER
            fht.StopSession();
        }

        public void RecoverAndTest(Guid cprVersion, Guid indexVersion)
        {
            // Recover
            fht.Recover(cprVersion, indexVersion);

            // Create array for reading
            Empty context;
            var inputArray = new Input[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                inputArray[i].adId.adId = i;
                inputArray[i].numClicks.numClicks = 0;
            }

            // Register with thread
            fht.StartSession();

            Input input = default(Input);
            Output output = default(Output);

            // Issue read requests
            for (var i = 0; i < numUniqueKeys; i++)
            {
                var status = fht.Read(ref inputArray[i].adId, ref input, ref output, ref context, i);
                Assert.IsTrue(status == Status.OK);
                inputArray[i].numClicks = output.value;
            }

            // Complete all pending requests
            fht.CompletePending(true);

            // Release
            fht.StopSession();

            // Set checkpoint directory
            Config.CheckpointDirectory = test_path;

            // Test outputs
            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(cprVersion);

            // Compute expected array
            long[] expected = new long[numUniqueKeys];
            foreach (var guid in checkpointInfo.continueTokens.Keys)
            {
                var sno = checkpointInfo.continueTokens[guid];
                for (long i = 0; i <= sno; i++)
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
                    "Debug error for AdId {0}: Expected ({1}), Found({2})", inputArray[i].adId.adId, expected[i], inputArray[i].numClicks.numClicks);
            }
        }
    }
}
