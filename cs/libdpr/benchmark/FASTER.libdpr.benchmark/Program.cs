using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FASTER.core;

namespace FASTER.libdpr
{
    class Program
    {
        private static int numWorkers = 100, numSimulatedVersions = 10000;
        private static double depProb = 1.0;
        private static Random rand = new Random();

        private static bool Finished(SimpleDprFinderBackend backend)
        {
            var cut = backend.GetVolatileState().GetCurrentCut();
            return cut.Count == numWorkers && cut.All(e => e.Value == numSimulatedVersions);
        }
        
        static void Main(string[] args)
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var testedBackend = new SimpleDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));

            for (var v = 1; v <= numSimulatedVersions; v++)
                SimulateVersion(v, testedBackend);

            var sw = Stopwatch.StartNew();
            while (!Finished(testedBackend))
                testedBackend.TryFindDprCut();
            
            sw.Stop();

            Console.WriteLine($"took {sw.ElapsedMilliseconds} milliseconds to process {numWorkers * numSimulatedVersions} worker-versions. Throughput: {(double) numWorkers * numSimulatedVersions / sw.ElapsedMilliseconds * 1000} wv/s");
        }

        private static void SimulateVersion(int v, SimpleDprFinderBackend backend)
        {
            var list = new List<WorkerVersion>();

            for (var i = 0; i < numWorkers; i++)
            {
                var wv = new WorkerVersion(new Worker(i), v);
                list.Clear();
                for (var j = 0; j < numWorkers; j++)
                {
                    if (i == j) continue;
                    if (rand.NextDouble() < depProb)
                        list.Add(new WorkerVersion(new Worker(j), v));
                    
                }
                backend.NewCheckpoint(wv, list);
            }
        }
    }
}