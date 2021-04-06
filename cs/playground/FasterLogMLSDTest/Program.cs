using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FasterLogMLSDTest
{
    public class Program
    {
        private static FasterLog log;
        private static IDevice device;
        static readonly byte[] entry = new byte[100];
        private static string commitPath;

        public static void Main()
        {
            commitPath = "FasterLogMLSDTest/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            // We loop to ensure clean-up as deleteOnClose does not always work for MLSD
            while (Directory.Exists(commitPath))
                Directory.Delete(commitPath, true);

            // Create devices \ log for test
            device = new ManagedLocalStorageDevice(commitPath + "ManagedLocalStore.log", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 12, MemorySizeBits = 14 });

            ManagedLocalStoreBasicTest();

            log.Dispose();
            device.Dispose();

            // Clean up log files
            if (Directory.Exists(commitPath))
                Directory.Delete(commitPath, true);
        }


        public static void ManagedLocalStoreBasicTest()
        {
            int entryLength = 20;
            int numEntries = 500_000;
            int numEnqueueThreads = 1;
            int numIterThreads = 1;
            bool commitThread = false;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            bool disposeCommitThread = false;
            var commit =
                new Thread(() =>
                {
                    while (!disposeCommitThread)
                    {
                        Thread.Sleep(10);
                        log.Commit(true);
                    }
                });

            if (commitThread)
                commit.Start();

            Thread[] th = new Thread[numEnqueueThreads];
            for (int t = 0; t < numEnqueueThreads; t++)
            {
                th[t] =
                new Thread(() =>
                {
                    // Enqueue but set each Entry in a way that can differentiate between entries
                    for (int i = 0; i < numEntries; i++)
                    {
                        // Flag one part of entry data that corresponds to index
                        entry[0] = (byte)i;

                        // Default is add bytes so no need to do anything with it
                        log.Enqueue(entry);
                    }
                });
            }

            Console.WriteLine("Populating log...");
            var sw = Stopwatch.StartNew();

            for (int t = 0; t < numEnqueueThreads; t++)
                th[t].Start();
            for (int t = 0; t < numEnqueueThreads; t++)
                th[t].Join();

            sw.Stop();
            Console.WriteLine($"{numEntries} items enqueued to the log by {numEnqueueThreads} threads in {sw.ElapsedMilliseconds} ms");

            if (commitThread)
            {
                disposeCommitThread = true;
                commit.Join();
            }

            // Final commit to the log
            log.Commit(true);

            // flag to make sure data has been checked 
            bool datacheckrun = false;

            Thread[] th2 = new Thread[numIterThreads];
            for (int t = 0; t < numIterThreads; t++)
            {
                th2[t] =
                    new Thread(() =>
                    {
                        // Read the log - Look for the flag so know each entry is unique
                        int currentEntry = 0;
                        using (var iter = log.Scan(0, long.MaxValue))
                        {
                            while (iter.GetNext(out byte[] result, out _, out _))
                            {
                                // set check flag to show got in here
                                datacheckrun = true;

                                if (numEnqueueThreads == 1)
                                    if (result[0] != (byte)currentEntry)
                                        throw new Exception("Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString());
                                currentEntry++;
                            }
                        }

                        if (currentEntry != numEntries * numEnqueueThreads)
                            throw new Exception("Error");
                    });
            }

            sw.Restart();

            for (int t = 0; t < numIterThreads; t++)
                th2[t].Start();
            for (int t = 0; t < numIterThreads; t++)
                th2[t].Join();

            sw.Stop();
            Console.WriteLine($"{numEntries} items iterated in the log by {numIterThreads} threads in {sw.ElapsedMilliseconds} ms");

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                throw new Exception("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }
    }
}
