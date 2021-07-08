using System;
using System.Collections.Generic;
using FASTER.libdpr;

namespace DprMicrobench
{
    public interface IWorkloadGenerator
    {
        List<WorkerVersion> GenerateDependenciesOneRun(IList<Worker> workers, Worker me, long currentVersion);
    }

    public class UniformWorkloadGenerator : IWorkloadGenerator
    {
        private List<WorkerVersion> dependecies = new List<WorkerVersion>();
        private Random rand = new Random();
        private double depProb;

        public UniformWorkloadGenerator(double depProb)
        {
            this.depProb = depProb;
        }
        
        public List<WorkerVersion> GenerateDependenciesOneRun(IList<Worker> workers, Worker me,long currentVersion)
        {
            dependecies.Clear();
            foreach (var worker in workers)
            {
                if (worker.Equals(me)) continue;

                if (rand.NextDouble() < depProb)
                    dependecies.Add(new WorkerVersion(worker, currentVersion));
            }

            return dependecies;
        }
    }

    public class SkewedWorkloadGenerator : IWorkloadGenerator
    {
        private List<WorkerVersion> dependecies = new List<WorkerVersion>();
        private Random rand = new Random();
        private double depProb, heavyHitterProb;
        private Worker heavyHitter;

        public SkewedWorkloadGenerator(double depProb, double heavyHitterProb, Worker heavyHitter)
        {
            this.depProb = depProb;
            this.heavyHitterProb = heavyHitterProb;
            this.heavyHitter = heavyHitter;
        }

        public List<WorkerVersion> GenerateDependenciesOneRun(IList<Worker> workers, Worker me,long currentVersion)
        {
            dependecies.Clear();
            foreach (var worker in workers)
            {
                if (worker.Equals(me)) continue;
                var flip = rand.NextDouble();
                if (worker.guid == heavyHitter.guid)
                {
                    if (flip < heavyHitterProb )
                        dependecies.Add(new WorkerVersion(worker, currentVersion));
                }
                else if (rand.NextDouble() < depProb)
                {
                    dependecies.Add(new WorkerVersion(worker, currentVersion));

                }
            }

            return dependecies;
        }
    }
}