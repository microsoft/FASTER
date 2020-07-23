// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Microsoft.Win32.SafeHandles;
using NUnit.Framework;
using NUnit.Framework.Internal;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.test.recovery.sumstore
{
    [TestFixture]
    internal class SharedDirectoryTests
    {
        const long numUniqueKeys = (1 << 14);
        const long keySpace = (1L << 14);
        const long numOps = (1L << 19);
        const long completePendingInterval = (1L << 10);
        private string rootPath;
        private string sharedLogDirectory;
        FasterTestInstance original;
        FasterTestInstance clone;

        [SetUp]
        public void Setup()
        {
            this.rootPath = $"{TestContext.CurrentContext.TestDirectory}\\{Path.GetRandomFileName()}";
            Directory.CreateDirectory(this.rootPath);
            this.sharedLogDirectory = $"{this.rootPath}\\SharedLogs";
            Directory.CreateDirectory(this.sharedLogDirectory);

            this.original = new FasterTestInstance();
            this.clone = new FasterTestInstance();
        }

        [TearDown]
        public void TearDown()
        {
            this.original.TearDown();
            this.clone.TearDown();
            try
            {
                Directory.Delete(this.rootPath, recursive: true);
            }
            catch
            {
            }
        }

        [Test]
        public void SharedLogDirectory()
        {
            this.original.Initialize($"{this.rootPath}\\OriginalCheckpoint", this.sharedLogDirectory);
            Assert.IsTrue(IsDirectoryEmpty(this.sharedLogDirectory)); // sanity check
            Populate(this.original.Faster);

            // Take checkpoint from original to start the clone from
            Assert.IsTrue(this.original.Faster.TakeFullCheckpoint(out var checkpointGuid));
            this.original.Faster.CompleteCheckpointAsync().GetAwaiter().GetResult();

            // Sanity check against original
            Assert.IsFalse(IsDirectoryEmpty(this.sharedLogDirectory));
            Test(this.original, checkpointGuid);

            // Copy checkpoint directory
            var cloneCheckpointDirectory = $"{this.rootPath}\\CloneCheckpoint";
            CopyDirectory(new DirectoryInfo(this.original.CheckpointDirectory), new DirectoryInfo(cloneCheckpointDirectory));

            // Recover from original checkpoint
            this.clone.Initialize(cloneCheckpointDirectory, this.sharedLogDirectory, populateLogHandles: true);
            this.clone.Faster.Recover(checkpointGuid);

            // Both sessions should work concurrently
            Test(this.original, checkpointGuid);
            Test(this.clone, checkpointGuid);

            // Dispose original, files should not be deleted
            this.original.TearDown();

            // Clone should still work
            Assert.IsFalse(IsDirectoryEmpty(this.sharedLogDirectory));
            Test(this.clone, checkpointGuid);

            this.clone.TearDown();

            // Files should be deleted after both instances are closed
            Assert.IsTrue(IsDirectoryEmpty(this.sharedLogDirectory));
        }

        private struct FasterTestInstance
        {
            public string CheckpointDirectory { get; private set; }
            public string LogDirectory { get; private set; }
            public FasterKV<AdId, NumClicks, AdInput, Output, Empty, Functions> Faster { get; private set; }
            public IDevice LogDevice { get; private set; }

            public void Initialize(string checkpointDirectory, string logDirectory, bool populateLogHandles = false)
            {
                this.CheckpointDirectory = checkpointDirectory;
                this.LogDirectory = logDirectory;

                string logFileName = "log";
                string deviceFileName = $"{this.LogDirectory}\\{logFileName}";
                KeyValuePair<int, SafeFileHandle>[] initialHandles = null;
                if (populateLogHandles)
                {
                    var segmentIds = new List<int>();
                    foreach (System.IO.FileInfo item in new DirectoryInfo(logDirectory).GetFiles(logFileName + "*"))
                    {
                        segmentIds.Add(int.Parse(item.Name.Replace(logFileName, "").Replace(".", "")));
                    }
                    segmentIds.Sort();
                    initialHandles = new KeyValuePair<int, SafeFileHandle>[segmentIds.Count];
                    for (int i = 0; i < segmentIds.Count; i++)
                    {
                        var segmentId = segmentIds[i];
                        var handle = LocalStorageDevice.CreateHandle(segmentId, disableFileBuffering: false, deleteOnClose: true, preallocateFile: false, segmentSize: -1, fileName: deviceFileName);
                        initialHandles[i] = new KeyValuePair<int, SafeFileHandle>(segmentId, handle);
                    }
                }

                this.LogDevice = new LocalStorageDevice(deviceFileName, deleteOnClose: true, disableFileBuffering: false, initialLogFileHandles: initialHandles);
                this.Faster = new FasterKV<AdId, NumClicks, AdInput, Output, Empty, Functions>(
                    keySpace,
                    new Functions(),
                    new LogSettings { LogDevice = this.LogDevice },
                    new CheckpointSettings { CheckpointDir = this.CheckpointDirectory, CheckPointType = CheckpointType.FoldOver });
            }

            public void TearDown()
            {
                this.Faster?.Dispose();
                this.Faster = null;
                this.LogDevice?.Close();
                this.LogDevice = null;
            }
        }

        private void Populate(FasterKV<AdId, NumClicks, AdInput, Output, Empty, Functions> fasterInstance)
        {
            using var session = fasterInstance.NewSession();

            // Prepare the dataset
            var inputArray = new AdInput[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId.adId = i % numUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            // Process the batch of input data
            for (int i = 0; i < numOps; i++)
            {
                session.RMW(ref inputArray[i].adId, ref inputArray[i], Empty.Default, i);

                if (i % completePendingInterval == 0)
                {
                    session.CompletePending(false);
                }
            }

            // Make sure operations are completed
            session.CompletePending(true);
        }

        private void Test(FasterTestInstance fasterInstance, Guid checkpointToken)
        {
            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(checkpointToken, new LocalCheckpointManager(fasterInstance.CheckpointDirectory));

            // Create array for reading
            var inputArray = new AdInput[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                inputArray[i].adId.adId = i;
                inputArray[i].numClicks.numClicks = 0;
            }

            var input = default(AdInput);
            var output = default(Output);

            using var session = fasterInstance.Faster.NewSession();
            // Issue read requests
            for (var i = 0; i < numUniqueKeys; i++)
            {
                var status = session.Read(ref inputArray[i].adId, ref input, ref output, Empty.Default, i);
                Assert.IsTrue(status == Status.OK);
                inputArray[i].numClicks = output.value;
            }

            // Complete all pending requests
            session.CompletePending(true);


            // Compute expected array
            long[] expected = new long[numUniqueKeys];
            foreach (var guid in checkpointInfo.continueTokens.Keys)
            {
                var sno = checkpointInfo.continueTokens[guid].UntilSerialNo;
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

            // Assert that expected is same as found
            for (long i = 0; i < numUniqueKeys; i++)
            {
                Assert.IsTrue(
                    expected[i] == inputArray[i].numClicks.numClicks,
                    "Debug error for AdId {0}: Expected ({1}), Found({2})", inputArray[i].adId.adId, expected[i], inputArray[i].numClicks.numClicks);
            }
        }

        private bool IsDirectoryEmpty(string path) => !Directory.Exists(path) || !Directory.EnumerateFileSystemEntries(path).Any();

        private static void CopyDirectory(DirectoryInfo source, DirectoryInfo target)
        {
            // Copy each file
            foreach (var file in source.GetFiles())
            {
                file.CopyTo(Path.Combine(target.FullName, file.Name), true);
            }

            // Copy each subdirectory
            foreach (var sourceSubDirectory in source.GetDirectories())
            {
                var targetSubDirectory = target.CreateSubdirectory(sourceSubDirectory.Name);
                CopyDirectory(sourceSubDirectory, targetSubDirectory);
            }
        }
    }
}
