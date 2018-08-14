// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using FASTER.core;

namespace FASTER.test.recovery.sumstore
{

    [TestClass]
    public class FullRecoveryTests
    {
        const long numUniqueKeys = (1 << 14);
        const long keySpace = (1L << 14);
        const long numOps = (1L << 19);
        const long refreshInterval = (1L << 8);
        const long completePendingInterval = (1L << 10);
        const long checkpointInterval = (1L << 16);
        private ICustomFaster fht;
        private string test_path;
        private Guid token;

        [TestInitialize]
        public void Setup()
        {
            if (test_path == null)
            {
                test_path = Path.GetTempPath() + Path.GetRandomFileName();
                if (!Directory.Exists(test_path))
                    Directory.CreateDirectory(test_path);
            }

            var log = FASTERFactory.CreateLogDevice(test_path + "\\hlog");

            fht = 
                FASTERFactory.Create
                <AdId, NumClicks, Input, Output, Empty, Functions, ICustomFaster>
                (keySpace, log, checkpointDir: test_path);
        }

        [TestCleanup]
        public void TearDown()
        {
            fht.StopSession();
            fht = null;
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

        [TestMethod]
        public void RecoveryTest1()
        {
            Populate();
            Setup();
            RecoverAndTest(token, token);
        }

        public unsafe void Populate()
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

            Guid nextToken;
            // Prpcess the batch of input data
            bool first = true;
            fixed (Input* input = inputArray)
            {
                for (int i = 0; i < numOps; i++)
                {
                    fht.RMW(&((input + i)->adId), input + i, &context, i);

                    if ((i+1) % checkpointInterval == 0)
                    {
                        if (first)
                            while (!fht.TakeFullCheckpoint(out token))
                                fht.Refresh();
                        else
                            while (!fht.TakeFullCheckpoint(out nextToken))
                                fht.Refresh();

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
            }

            // Make sure operations are completed
            fht.CompletePending(true);

            // Deregister thread from FASTER
            fht.StopSession();
        }

        public unsafe void RecoverAndTest(Guid cprVersion, Guid indexVersion)
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

            // Issue read requests
            fixed (Input* input = inputArray)
            {
                for (var i = 0; i < numUniqueKeys; i++)
                {
                    fht.Read(&((input + i)->adId), null, (Output*)&((input + i)->numClicks), &context, i);
                }
            }

            // Complete all pending requests
            fht.CompletePending(true);

            // Release
            fht.StopSession();

            // Test outputs
            Config.CheckpointDirectory = test_path;
            var recoveryInfo = default(HybridLogRecoveryInfo);
            recoveryInfo.Recover(cprVersion);

            int num_threads = recoveryInfo.numThreads;
            DirectoryInfo info = new DirectoryInfo(DirectoryConfiguration.GetHybridLogCheckpointFolder(cprVersion));
            List<ExecutionContext> cpr_points = new List<ExecutionContext>();
            foreach (var file in info.GetFiles())
            {
                if (file.Name != "info.dat" && file.Name != "snapshot.dat")
                {
                    using (var reader = new StreamReader(file.FullName))
                    {
                        var ctx = new ExecutionContext();
                        ctx.Load(reader);
                        cpr_points.Add(ctx);
                    }
                }
            }

            // Compute expected array
            long[] expected = new long[numUniqueKeys];
            long[] found = new long[numUniqueKeys];
            long sno = cpr_points.First().serialNum;
            for (long i = 0; i <= sno; i++)
            {
                var id = i % numUniqueKeys;
                expected[id]++;
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
