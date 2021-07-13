﻿

using System;
using System.Collections.Generic;
using CommandLine;
using FASTER.benchmark;
using FASTER.libdpr;

namespace DprMicrobench
{
class Options
    {
        [Option('t', "type", Required = true, HelpText = "worker or coordinator")]
        public string Type { get; set; }

        [Option('c', "worker-count", Required = false, Default = 8,
            HelpText = "Number of dpr workers to simulate")]
        public int WorkerCount { get; set; }
        
        [Option('n', "worker-id", Required = false,
            HelpText = "worker id")]
        public int WorkerId { get; set; }

        [Option('v', "dpr-type", Required = false, Default = "v1",
            HelpText = "type of dpr protocol to use (v1, v2 or v3)")]
        public string DprType { get; set; }
        
        [Option('p', "dependency-probability", Required = false, Default = 0.5,
            HelpText = "probability to depend on a worker")]
        public double DependencyProbability { get; set; }

        [Option('h', "heavy-hitter-probability", Required = false, Default = 0.0,
            HelpText = "probability to depend on a singular heavy-hitter")]
        public double HeavyHitterProbability { get; set; }
        
        [Option('d', "delay-prob", Required = false, Default = 0.0)]
        public double DelayProbability { get; set; }
        
        [Option('i', "checkpoint_interval", Required = false, Default = 25)]
        public int CheckpointInterval { get; set; }

        [Option('r', "run_length", Required = false, Default = 30)]
        public int RunLength { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) throw new Exception();

            var options = result.MapResult(o => o, xs => new Options());
            if (options.Type.Equals("coordinator"))
            {
                var workers = new List<Worker>();
                for (var i = 0; i < options.WorkerCount; i++)
                    workers.Add(new Worker(i));
                // assign simulated worker to pod with round-robin
                var assignment = new Dictionary<long, List<Worker>>();
                foreach (var pod in DprCoordinator.clusterConfig.pods)
                    assignment.Add(pod.worker.guid, new List<Worker>());
                var podId = 0;
                foreach (var worker in workers)
                {
                    assignment[podId].Add(worker);
                    podId++;
                    if (podId >= assignment.Count) podId = 0;
                }
                
                var config = new BenchmarkConfiguration
                {
                    workers = workers,
                    assignment = assignment,
                    dprType = options.DprType,
                    depProb = options.DependencyProbability,
                    heavyHitterProb = options.HeavyHitterProbability,
                    delayProb = options.DelayProbability,
                    averageMilli = options.CheckpointInterval,
                    delayMilli = options.CheckpointInterval / 10,
                    connString = "",
                    runSeconds = options.RunLength
                };
                var c = new DprCoordinator(config);
                c.Run();
            }
            else if (options.Type.Equals("worker"))
            {
                var pod = new DprWorkerPod(options.WorkerId);
                pod.Run();
            }
            else
            {
                throw new Exception();
            }
        }
    }
}