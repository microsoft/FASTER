// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class MoreLogCompactionTests
    {
        private FasterKV<long, long> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait:true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/MoreLogCompactionTests.log", deleteOnClose: true);
            fht = new FasterKV<long, long>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9 });
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]
        [Category("Smoke")]

        public void DeleteCompactLookup([Values] CompactionType compactionType)
        {
            using var session = fht.NewSession(new SimpleFunctions<long, long>());

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1010)
                    compactUntil = fht.Log.TailAddress;
                session.Upsert(i, i);
            }

            for (int i = 0; i < totalRecords / 2; i++)
                session.Delete(i);

            compactUntil = session.Compact(compactUntil, compactionType);

            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);

            using var session2 = fht.NewSession(new SimpleFunctions<long, long>());

            // Verify records by reading
            for (int i = 0; i < totalRecords; i++)
            {
                (var status, var output) = session2.Read(i);
                if (status.IsPending)
                {
                    session2.CompletePendingWithOutputs(out var completedOutputs, true);
                    Assert.IsTrue(completedOutputs.Next());
                    (status, output) = (completedOutputs.Current.Status, completedOutputs.Current.Output);
                    Assert.IsFalse(completedOutputs.Next());
                }

                if (i < totalRecords / 2)
                {
                    Assert.IsTrue(status.NotFound);
                }
                else
                {
                    Assert.IsTrue(status.Found);
                    Assert.AreEqual(i, output);
                }
            }
        }
    }
}
