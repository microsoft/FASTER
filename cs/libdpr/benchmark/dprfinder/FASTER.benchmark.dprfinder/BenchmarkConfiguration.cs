using System;
using System.Collections.Generic;
using FASTER.libdpr;

namespace FASTER.benchmark
{
    [Serializable]
    public class BenchmarkConfiguration
    {
        public List<Worker> workers;
        public Dictionary<long, List<Worker>> assignment;
        public string dprType;
        public double depProb, heavyHitterProb, delayProb;
        public int averageMilli, delayMilli;
        public string connString;
        public int runSeconds;

        public override string ToString()
        {
            return $"worker num:{workers.Count}, {nameof(dprType)}: {dprType}, {nameof(depProb)}: {depProb}, {nameof(heavyHitterProb)}: {heavyHitterProb}, {nameof(delayProb)}: {delayProb}, {nameof(averageMilli)}: {averageMilli}, {nameof(delayMilli)}: {delayMilli}, {nameof(runSeconds)}: {runSeconds}";
        }
    }
}