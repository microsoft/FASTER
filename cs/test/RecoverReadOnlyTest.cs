// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;


namespace FASTER.test
{

    [TestFixture]
    internal class BasicRecoverReadOnly
    {
        //private FasterLog log;
       // private IDevice device;
        private FasterLog logReadOnly;
        private IDevice deviceReadOnly;
        private FasterLog log;
        private IDevice device;

        private static string path = Path.GetTempPath() + "BasicRecoverReadOnly/";
        const int commitPeriodMs = 2000;
        const int restorePeriodMs = 1000;


        [SetUp]
        public void Setup()
        {

            // Clean up log files from previous test runs in case they weren't cleaned up
            try {  new DirectoryInfo(path).Delete(true);  }
            catch {}

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "Recover", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
//            var device = Devices.CreateLogDevice(path + "mylog");
  //          var log = new FasterLog(new FasterLogSettings { LogDevice = device, MemorySizeBits = 11, PageSizeBits = 9, MutableFraction = 0.5, SegmentSizeBits = 9 });



            // Run consumer on SEPARATE read-only FasterLog instance - need this for recovery
            //deviceReadOnly = Devices.CreateLogDevice(path + "ReadOnly", deleteOnClose: true);
            //logReadOnly = new FasterLog(new FasterLogSettings { LogDevice = deviceReadOnly, ReadOnlyMode = true });
        }

        [TearDown]
        public void TearDown()
        {
            log.Dispose();
            logReadOnly.Dispose();

            // Clean up log files
            try { new DirectoryInfo(path).Delete(true); }
            catch { }
        }


        [Test]
        public async ValueTask RecoverReadOnlyBasicTest()
        {

            using var cts = new CancellationTokenSource();

            var producer = ProducerAsync(log, cts.Token);
            var commiter = CommitterAsync(log, cts.Token);

            // Run consumer on SEPARATE read-only FasterLog instance
            var consumer = SeparateConsumerAsync(cts.Token);

//            Console.CancelKeyPress += (o, eventArgs) =>
  //          {
                Console.WriteLine("Cancelling program...");
      //          eventArgs.Cancel = true;
                //cts.Cancel();
    //        };

            //await producer;
            //await consumer;
            //await commiter;

//            Console.WriteLine("Finished.");

            log.Dispose();
            try { new DirectoryInfo(path).Delete(true); } catch { }

        }

        static async Task CommitterAsync(FasterLog log, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(commitPeriodMs), cancellationToken);

                Console.WriteLine("Committing...");

                await log.CommitAsync();
            }
        }

        static async Task ProducerAsync(FasterLog log, CancellationToken cancellationToken)
        {
            var i = 0L;
            while (!cancellationToken.IsCancellationRequested)
            {

                byte[] entry = new byte[100];
                entry[i] = (byte)i;

                log.Enqueue(entry);
                log.RefreshUncommitted();

                i++;

                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
        }

        static async Task SeparateConsumerAsync(CancellationToken cancellationToken)
        {
            //            var device = Devices.CreateLogDevice(path + "mylog");
            //          var log = new FasterLog(new FasterLogSettings { LogDevice = device, ReadOnlyMode = true, PageSizeBits = 9, SegmentSizeBits = 9 });
            var deviceReadOnly = Devices.CreateLogDevice(path + "ReadOnly", deleteOnClose: true);
            var logReadOnly = new FasterLog(new FasterLogSettings { LogDevice = deviceReadOnly, ReadOnlyMode = true });


            //var _ = RecoverAsync(log, cancellationToken);

            using var iter = logReadOnly.Scan(logReadOnly.BeginAddress, long.MaxValue);

            await foreach (var (result, length, currentAddress, nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
            {
             
                string DG23 = result.ToString();
                iter.CompleteUntil(nextAddress);
            }
        }

        static async Task RecoverAsync(FasterLog log, CancellationToken cancellationToken)
        {
//            while (!cancellationToken.IsCancellationRequested)
  //          {
//                await Task.Delay(TimeSpan.FromMilliseconds(restorePeriodMs), cancellationToken);
                
                log.RecoverReadOnly();
    //        }
        }






    }
}


