using System;

namespace FASTER.libdpr
{
    /// <summary>
    ///     A worker in the system manipulates uniquely exactly one state object.
    /// </summary>
    [Serializable]
    public struct Worker
    {
        public static readonly Worker INVALID = new Worker(-1);
        /// <summary>
        ///     Reserved Worker id for cluster management
        /// </summary>
        public readonly long guid;


        /// <summary>
        ///     Constructs a worker with the given guid
        /// </summary>
        /// <param name="guid"> worker guid </param>
        public Worker(long guid)
        {
            this.guid = guid;
        }

        internal bool Equals(Worker other)
        {
            return guid == other.guid;
        }

        /// <inheritdoc cref="object" />
        public override bool Equals(object obj)
        {
            return obj is Worker other && Equals(other);
        }

        /// <inheritdoc cref="object" />
        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }
    }

    /// <summary>
    ///     A worker-version is a tuple of worker and checkpoint version.
    /// </summary>
    public struct WorkerVersion
    {
        /// <summary>
        ///     Worker
        /// </summary>
        public Worker Worker { get; set; }

        /// <summary>
        ///     Version
        /// </summary>
        public long Version { get; set; }

        /// <summary>
        ///     Constructs a new worker version object with given parameters
        /// </summary>
        /// <param name="worker">worker</param>
        /// <param name="version">version</param>
        public WorkerVersion(Worker worker, long version)
        {
            Worker = worker;
            Version = version;
        }

        internal WorkerVersion(long worker, long version) : this(new Worker(worker), version)
        {
        }

        internal bool Equals(WorkerVersion other)
        {
            return Worker.Equals(other.Worker) && Version == other.Version;
        }

        /// <inheritdoc cref="object" />
        public override bool Equals(object obj)
        {
            return obj is WorkerVersion other && Equals(other);
        }

        /// <inheritdoc cref="object" />
        public override int GetHashCode()
        {
            unchecked
            {
                return (Worker.GetHashCode() * 397) ^ Version.GetHashCode();
            }
        }
    }
}