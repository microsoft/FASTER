// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{

    [TestFixture]
    internal class FasterLogTests
    {
        const int entryLength = 100;
        const int numEntries = 1000000;
        private FasterLog log;
        private IDevice device;


        [SetUp]
        public void Setup()
        {
            device = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\fasterlog.log", deleteOnClose: true);
        }

        [TearDown]
        public void TearDown()
        {
            device.Close();
        }

        [Test]
        public void FasterLogTest1()
        {
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
            log.AcquireThread();

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            for (int i = 0; i < numEntries; i++)
            {
                log.Append(entry);
            }
            log.FlushAndCommit(true);

            using (var iter = log.Scan(0, long.MaxValue))
            {
                int count = 0;
                while (iter.GetNext(out Span<byte> result, out int length))
                {
                    count++;
                    Assert.IsTrue(result.SequenceEqual(entry));
                    if (count % 100 == 0)
                        log.TruncateUntil(iter.CurrentAddress);
                }
                Assert.IsTrue(count == numEntries);
            }

            log.ReleaseThread();
            log.Dispose();
        }
    }
}
