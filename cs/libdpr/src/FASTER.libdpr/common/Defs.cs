using System;

namespace FASTER.libdpr
{
    // TODO(Tianyu): Document
    public struct Worker
    {
        public static Worker CLUSTER_MANAGER = new Worker(0);
        public readonly long guid;

        public Worker(long guid)
        {
            this.guid = guid;
        }

        public bool Equals(Worker other)
        {
            return guid == other.guid;
        }

        public override bool Equals(object obj)
        {
            return obj is Worker other && Equals(other);
        }

        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }
    }
    
    public struct WorkerVersion
    {
        public Worker Worker { get; set; }
        public long Version { get; set; }

        public WorkerVersion(Worker worker, long version)
        {
            Worker = worker;
            Version = version;
        }

        public WorkerVersion(long worker, long version) : this(new Worker(worker), version)
        {
        }

        public bool Equals(WorkerVersion other)
        {
            return Worker.Equals(other.Worker) && Version == other.Version;
        }

        public override bool Equals(object obj)
        {
            return obj is WorkerVersion other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Worker.GetHashCode() * 397) ^ Version.GetHashCode();
            }
        }
    }
}