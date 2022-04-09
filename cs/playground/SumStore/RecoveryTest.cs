using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using FASTER.core;

namespace SumStore
{
    class RecoveryTest
    {
        const long numUniqueKeys = 1 << 23;
        const long indexSize = 1L << 26;
        const long numOps = 4 * numUniqueKeys;
        const long refreshInterval = 1 << 8;
        const long completePendingInterval = 1 << 12;
        const int checkpointInterval = 10 * 1000;
        readonly int threadCount;
        readonly FasterKV<AdId, NumClicks> fht;

        readonly BlockingCollection<Input[]> inputArrays;

        public RecoveryTest(int threadCount)
        {
            this.threadCount = threadCount;

            // Create FASTER index
            var log = Devices.CreateLogDevice("logs/hlog");

            FasterKVSettings<AdId, NumClicks> fkvSettings = new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                CheckpointDir = "logs"
            };

            fht = new(fkvSettings);

            inputArrays = new BlockingCollection<Input[]>();
            Prepare();
        }

        public void Populate()
        {
            Run(false);
        }

        public void Continue()
        {
            fht.Recover();
            Run(true);
        }

        public void RecoverLatest()
        {
            fht.Recover();
            Test();
        }

        public void Recover(Guid indexToken, Guid hybridLogToken)
        {
            fht.Recover(indexToken, hybridLogToken);
            Test();
        }

        private void Prepare()
        {
            Thread[] workers = new Thread[threadCount];
            for (int idx = 0; idx < threadCount; ++idx)
            {
                int x = idx;
                workers[idx] = new Thread(() => PrepareThread());
            }

            foreach (Thread worker in workers)
                worker.Start();

            foreach (Thread worker in workers)
                worker.Join();
        }

        private void PrepareThread()
        {
            var inputArray = new Input[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId.adId = i % numUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            inputArrays.Add(inputArray);
        }

        private void Run(bool continueSession)
        {
            // Create a thread to issue periodic checkpoints
            var t = new Thread(() => PeriodicCheckpoints());
            t.Start();

            Thread[] workers = new Thread[threadCount];
            for (int idx = 0; idx < threadCount; ++idx)
            {
                int x = idx;
                workers[idx] = new Thread(() => RunThread(x, continueSession));
            }

            foreach (Thread worker in workers)
                worker.Start();
            foreach (Thread worker in workers)
                worker.Join();
        }

        private void RunThread(int threadId, bool continueSession)
        {
            ClientSession<AdId, NumClicks, Input, Output, Empty, Functions, DefaultStoreFunctions<AdId, NumClicks>> session;
            long sno = 0;
            if (continueSession)
            {
                // Register with thread
                do
                {
                    session = fht.For(new Functions()).ResumeSession<Functions>(threadId.ToString(), out CommitPoint cp);
                    sno = cp.UntilSerialNo;
                } while (sno == -1);
                Console.WriteLine("Session {0} recovered until {1}", threadId, sno);
                sno++;
            }
            else
            {
                // Register thread with FASTER
                session = fht.For(new Functions()).NewSession<Functions>(threadId.ToString());
            }

            GenerateClicks(session, sno);
        }
        private void GenerateClicks(ClientSession<AdId, NumClicks, Input, Output, Empty, Functions, DefaultStoreFunctions<AdId, NumClicks>> session, long sno)
        {
            inputArrays.TryTake(out Input[] inputArray);

            // Increment in round-robin fashion starting from sno
            while (true)
            {
                var key = (sno % numUniqueKeys);
                session.RMW(ref inputArray[key].adId, ref inputArray[key], Empty.Default, sno);

                sno++;

                if (sno % refreshInterval == 0)
                {
                    session.Refresh();
                }
                else if (sno % completePendingInterval == 0)
                {
                    session.CompletePending(false);
                }
                else if (sno % numUniqueKeys == 0)
                {
                    session.CompletePending(true);
                }
            }
        }

        private void PeriodicCheckpoints()
        {

            Console.WriteLine("Started checkpoint thread");

            while(true)
            {
                Thread.Sleep(checkpointInterval);

                fht.TryInitiateFullCheckpoint(out Guid token, CheckpointType.Snapshot);
                fht.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

                Console.WriteLine("Completed checkpoint {0}", token);
            }
        }

        private void Test()
        {
            List<long> sno = new();

            for (int i = 0; i < threadCount; i++)
            {
                var s = fht.For(new Functions()).ResumeSession<Functions>(i.ToString(), out CommitPoint cp);
                sno.Add(cp.UntilSerialNo);
                Console.WriteLine("Session {0} recovered until {1}", i, cp.UntilSerialNo);
                s.Dispose();
            }


            // Initalize array to store values
            Input[] inputArray = new Input[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                inputArray[i].adId.adId = i % numUniqueKeys;
                inputArray[i].numClicks.numClicks = 0;
            }

            // Start a new session
            var session = fht.For(new Functions()).NewSession<Functions>();

            // Issue read requests
            for (int i = 0; i < numUniqueKeys; i++)
            {
                Output output = default;
                var status = session.Read(ref inputArray[i].adId, ref inputArray[i], ref output, Empty.Default, i);
                Debug.Assert(status.IsCompletedSuccessfully);
                inputArray[i].numClicks.numClicks = output.value.numClicks;
            }

            // Complete all pending requests
            session.CompletePending(true);

            // Release
            session.Dispose();

            // Compute expected array
            long[] expected = new long[numUniqueKeys];
            foreach (var _sno in sno)
            {
                for (long i = 0; i <= _sno; i++)
                {
                    var id = i % numUniqueKeys;
                    expected[id]++;
                }
            }

            // Assert if expected is same as found
            for (long i = 0; i < numUniqueKeys; i++)
            {
                if (expected[i] != inputArray[i].numClicks.numClicks)
                {
                    Console.WriteLine("Debug error for AdId {0}: Expected ({1}), Found({2})", inputArray[i].adId.adId, expected[i], inputArray[i].numClicks.numClicks);
                }
            }
            Console.WriteLine("Test successful");
        }
    }
}
