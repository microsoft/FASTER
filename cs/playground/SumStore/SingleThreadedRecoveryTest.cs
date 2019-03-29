using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using FASTER.core;

namespace SumStore
{
    class SingleThreadedRecoveryTest : IFasterRecoveryTest
    {
        const long numUniqueKeys = (1 << 23);
        const long numOps = 4 * numUniqueKeys;
        const long refreshInterval = (1 << 8);
        const long completePendingInterval = (1 << 12);
        const int checkpointInterval = 10 * 1000;
        FasterKV<AdId, NumClicks, Input, Output, Empty, Functions> fht;

        Input[] inputArray;

        public SingleThreadedRecoveryTest()
        {
            // Create FASTER index
            var log = Devices.CreateLogDevice("logs\\hlog");
            fht = new FasterKV
                <AdId, NumClicks, Input, Output, Empty, Functions>
                (numUniqueKeys, new Functions(),
                new LogSettings { LogDevice = log },
                new CheckpointSettings { CheckpointDir = "logs" });   
        }

        public void Populate()
        {
            // Register thread with FASTER
            Guid sessionGuid = fht.StartSession();

            Directory.CreateDirectory("logs\\clients");
            // Persist session id
            System.IO.File.WriteAllText("logs\\clients\\0.txt", sessionGuid.ToString());

            // Create a thread to issue periodic checkpoints
            var t = new Thread(() => PeriodicCheckpoints());
            t.Start();

            // Generate clicks from start
            GenerateClicks(0);
        }

        public void Continue()
        {
            // Recover the latest checkpoint
            fht.Recover();

            // Find out session Guid
            string sessionGuidText = System.IO.File.ReadAllText("logs\\clients\\0.txt");
            Guid.TryParse(sessionGuidText, out Guid sessionGuid);

            // Register with thread
            long sno = fht.ContinueSession(sessionGuid);

            Console.WriteLine("Session {0} recovered until {1}", sessionGuid, sno);

            // Create a thread to issue periodic checkpoints
            var t = new Thread(() => PeriodicCheckpoints());
            t.Start();

            GenerateClicks(sno + 1);
        }

        public void RecoverAndTest()
        {
            // Recover the latest checkpoint
            fht.Recover();

            // Find out session Guid
            string sessionGuidText = System.IO.File.ReadAllText("logs\\clients\\0.txt");
            Guid.TryParse(sessionGuidText, out Guid sessionGuid);

            // Register with thread
            long sno = fht.ContinueSession(sessionGuid);

            Console.WriteLine("Session {0} recovered until {1}", sessionGuid, sno);

            // De-register session
            fht.StopSession();

            Test(sno);
        }

        private void GenerateClicks(long sno)
        {
            // Prepare the dataset
            inputArray = new Input[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                inputArray[i].adId.adId = (i % numUniqueKeys);
                inputArray[i].numClicks.numClicks = 1;
            }

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

        public void RecoverAndTest(Guid indexToken, Guid hybridLogToken)
        {
            // Recover the latest checkpoint
            fht.Recover(indexToken, hybridLogToken);

            // Find out session Guid
            string sessionGuidText = System.IO.File.ReadAllText("logs\\clients\\0.txt");
            Guid.TryParse(sessionGuidText, out Guid sessionGuid);

            // Register with thread
            long sno = fht.ContinueSession(sessionGuid);

            Console.WriteLine("Session {0} recovered until {1}", sessionGuid, sno);

            // De-register session
            fht.StopSession();

            Test(sno);

        }

        private void Test(long sno)
        {
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
            for (long i = 0; i <= sno; i++)
            {
                var id = i % numUniqueKeys;
                expected[id]++;
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

            Console.ReadLine();
        }
    }
}
