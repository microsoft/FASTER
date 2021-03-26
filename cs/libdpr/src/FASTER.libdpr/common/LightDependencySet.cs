using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    public class LightDependencySet : IEnumerable<WorkerVersion>
    {
        private const int MaxSizeBits = 8;
        /**
         * The maximum number of workers in a cluster this light dependency set can support. Compile-time constant.
         */
        public const int MaxClusterSize = 1 << MaxSizeBits;
        private const long NoDependency = -1;
        private readonly long[] dependentVersions;
        
        private class LightDependencySetEnumerator : IEnumerator<WorkerVersion>
        {
            private int index = -1;
            private readonly LightDependencySet dependencySet;

            public LightDependencySetEnumerator(LightDependencySet dependencySet)
            {
                this.dependencySet = dependencySet;
            }
            
            public bool MoveNext()
            {
                while (index++ < MaxClusterSize)
                    if (dependencySet.dependentVersions[index] != NoDependency) return true;
                return false;
            }

            public void Reset()
            {
                index = -1;
            }

            public WorkerVersion Current => new WorkerVersion(index, dependencySet.dependentVersions[index]);

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }

        public LightDependencySet()
        {
            dependentVersions = new long[1 << MaxSizeBits];
            for (var i = 0; i < dependentVersions.Length; i++)
                dependentVersions[i] = NoDependency;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Update(Worker worker, long version)
        {
            ref var originalVersion = ref dependentVersions[worker.guid];
            Utility.MonotonicUpdate(ref originalVersion, version, out _);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRemove(Worker worker, long version)
        {
            ref var originalVersion = ref dependentVersions[worker.guid];
            return Interlocked.CompareExchange(ref originalVersion, NoDependency, version) == version;
        }

        public IEnumerator<WorkerVersion> GetEnumerator()
        {
            return new LightDependencySetEnumerator(this);

        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}