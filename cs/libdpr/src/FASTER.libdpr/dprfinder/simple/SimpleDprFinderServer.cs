using System.Collections.Generic;
using FASTER.core;

namespace FASTER.libdpr
{
    public class SimpleDprFinderServer
    {
        private IDevice persistentStorage;
        private Dictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph;
        private Dictionary<Worker, long> persistedCut;
        private Queue<WorkerVersion> writes;
    }
}