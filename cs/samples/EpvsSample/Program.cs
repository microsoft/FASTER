using System;
using CommandLine;

namespace EpvsSample
{
    internal class Options
    {
        [Option('m', "synchronization-mode", Default = "epvs",
            HelpText = "synchronization mode options:" +
                       "\n    epvs" +
                       "\n    epvs-refresh" +
                       "\n    latch")]
        public string SynchronizationMode { get; set; } = null!;

        [Option('o', "num-ops", Default = 1000000)]
        public int NumOps { get; set; }

        [Option('t', "num-threads", Required = true)]
        public int NumThreads { get; set; }
        
        [Option('n', "numa", Required = false, Default = 0,
            HelpText = "NUMA options:" +
                       "\n    0 = No sharding across NUMA sockets" +
                       "\n    1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('p', "probability", Default = 1e-6)]
        public double VersionChangeProbability { get; set; }
        
        [Option('l', "delay", Default = 1)]
        public int VersionChangeDelay { get; set; }
        
                
        [Option('u', "output-file", Default = "")]
        public string OutputFile { get; set; } = null!;
    }


    internal class Program
    {
        static void Main(string[] args)
        {
            var options = Parser.Default.ParseArguments<Options>(args).Value;
            var bench = new EpvsBench();
            bench.RunExperiment(options);
        }
    }
}