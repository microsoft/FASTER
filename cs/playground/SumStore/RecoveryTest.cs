using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using FASTER.core;

namespace SumStore
{
    class RecoveryTest : IFasterRecoveryTest
    {
        const long numUniqueKeys = 1 << 23;
        const long indexSize = 1L << 20;
        const long numOps = 4 * numUniqueKeys;
        const long refreshInterval = 1 << 8;
        const long completePendingInterval = 1 << 12;
        const int checkpointInterval = 10 * 1000;
        readonly int threadCount;
        readonly int numActiveThreads;
        FasterKV<AdId, NumClicks, Input, Output, Empty, Functions> fht;

        BlockingCollection<Input[]> inputArrays;

        public RecoveryTest(int threadCount)
        {
            this.threadCount = threadCount;
            numActiveThreads = 0;

            // Create FASTER index
            var log = Devices.CreateLogDevice("logs\\hlog");
            fht = new FasterKV
                <AdId, NumClicks, Input, Output, Empty, Functions>
                (indexSize, new Functions(),
                new LogSettings { LogDevice = log },
                new CheckpointSettings { CheckpointDir = "logs" });

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
                workers[idx] = new Thread(() => PrepareThread(x));
            }

            foreach (Thread worker in workers)
                worker.Start();

            foreach (Thread worker in workers)
                worker.Join();
        }

        private void PrepareThread(int threadIdx)
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
            long sno = 0;
            if (continueSession)
            {
                // Find out session Guid
                string sessionGuidText = System.IO.File.ReadAllText("logs\\clients\\" + threadId + ".txt");
                Guid.TryParse(sessionGuidText, out Guid sessionGuid);

                // Register with thread
                sno = fht.ContinueSession(sessionGuid);

                Console.WriteLine("Session {0} recovered until {1}", sessionGuid, sno);
                sno++;
            }
            else
            {
                // Register thread with FASTER
                Guid sessionGuid = fht.StartSession();

                Directory.CreateDirectory("logs\\clients");
                // Persist session id
                File.WriteAllText("logs\\clients\\" + threadId + ".txt", sessionGuid.ToString());
            }

            GenerateClicks(sno);
        }
        private void GenerateClicks(long sno)
        {
            inputArrays.TryTake(out Input[] inputArray);

            // Increment in round-robin fashion starting from sno
            while (true)
            {
                var key = (sno % numUniqueKeys);
                fht.RMW(ref inputArray[key].adId, ref inputArray[key], Empty.Default, sno);

                sno++;

                if (sno % refreshInterval == 0)
                {
                    fht.Refresh();
                }
                else if (sno % completePendingInterval == 0)
                {
                    fht.CompletePending(false);
                }
                else if (sno % numUniqueKeys == 0)
                {
                    fht.CompletePending(true);
                }
            }
        }

        private void PeriodicCheckpoints()
        {

            Console.WriteLine("Started checkpoint thread");

            while(true)
            {
                Thread.Sleep(checkpointInterval);

                fht.TakeFullCheckpoint(out Guid token);

                fht.CompleteCheckpoint(true);

                Console.WriteLine("Completed checkpoint {0}", token);
            }
        }

        private void Test()
        {
            List<long> sno = new List<long>();

            for (int i = 0; i < threadCount; i++)
            {
                // Find out session Guid
                string sessionGuidText = System.IO.File.ReadAllText("logs\\clients\\" + i + ".txt");
                Guid.TryParse(sessionGuidText, out Guid sessionGuid);

                // Register with thread
                var _sno = fht.ContinueSession(sessionGuid);
                sno.Add(_sno);

                Console.WriteLine("Session {0} recovered until {1}", sessionGuid, _sno);

                // De-register session
                fht.StopSession();
            }


            // Initalize array to store values
            Input[] inputArray = new Input[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                inputArray[i].adId.adId = (i % numUniqueKeys);
                inputArray[i].numClicks.numClicks = 0;
            }

            // Start a new session
            fht.StartSession();

            // Issue read requests
            for (int i = 0; i < numUniqueKeys; i++)
            {
                Output output = default(Output);
                var status = fht.Read(ref inputArray[i].adId, ref inputArray[i], ref output, Empty.Default, i);
                Debug.Assert(status == Status.OK || status == Status.NOTFOUND);
                inputArray[i].numClicks.numClicks = output.value.numClicks;
            }

            // Complete all pending requests
            fht.CompletePending(true);

            // Release
            fht.StopSession();

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
