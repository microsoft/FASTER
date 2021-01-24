using System.Collections.Generic;
using System.Runtime.CompilerServices;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.iibdpr
{
    // TODO(Tianyu): Cannot support more than a handful of dependencies. Need to change for larger cluster size
    public class LightDependencySet
    {
        public const int MaxSizeBits = 4;
        public const long NoDependency = -1;
        private const int MaxSizeMask = (1 << MaxSizeBits) - 1;

        public readonly long[] DependentVersions;
        private bool maybeNotEmpty;

        public LightDependencySet()
        {
            DependentVersions = new long[1 << MaxSizeBits];
            for (var i = 0; i < DependentVersions.Length; i++)
                DependentVersions[i] = NoDependency;
            maybeNotEmpty = false;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Update(Worker worker, long version)
        {
            maybeNotEmpty = true;
            ref var originalVersion = ref DependentVersions[worker.guid & MaxSizeMask];
            Utility.MonotonicUpdate(ref originalVersion, version, out _);
        }

        public bool MaybeNotEmpty()
        {
            return maybeNotEmpty;
        }
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeClear()
        {
            for (var i = 0; i < DependentVersions.Length; i++)
                DependentVersions[i] = NoDependency;
            maybeNotEmpty = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeRemove(Worker worker, long version)
        {
            ref var originalVersion = ref DependentVersions[worker.guid & MaxSizeBits];
            if (originalVersion <= version) originalVersion = NoDependency;
        }

        public List<WorkerVersion> UnsafeToList()
        {
            var result = new List<WorkerVersion>();
            for (var i = 0; i < DependentVersions.Length; i++)
            {
                if (DependentVersions[i] == NoDependency) continue;
                result.Add(new WorkerVersion(new Worker(i), DependentVersions[i]));
            }

            return result;
        }
    }
}