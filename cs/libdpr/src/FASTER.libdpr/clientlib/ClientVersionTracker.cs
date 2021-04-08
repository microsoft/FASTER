using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using FASTER.core;

namespace FASTER.libdpr
{
    internal class ClientVersionTrackerEnumerator : IEnumerator<(long, WorkerVersion)>
    {
        private IEnumerator<KeyValuePair<WorkerVersion, List<long>>> versionEnumerator;
        private IEnumerator<long> opEnumerator;

        internal ClientVersionTrackerEnumerator(Dictionary<WorkerVersion, List<long>> dict)
        {
            versionEnumerator = dict.GetEnumerator();
        } 
        
        public bool MoveNext()
        {
            if (opEnumerator != null && opEnumerator.MoveNext()) return true;
            if (!versionEnumerator.MoveNext()) return false;
            opEnumerator?.Dispose();
            opEnumerator = versionEnumerator.Current.Value.GetEnumerator();
            return true;
        }

        public void Reset()
        {
            versionEnumerator.Reset();
            opEnumerator = versionEnumerator.Current.Value.GetEnumerator();
        }

        public (long, WorkerVersion) Current => ValueTuple.Create(opEnumerator.Current, versionEnumerator.Current.Key);

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            versionEnumerator.Dispose();
        }
    }
    
    // TODO(Tianyu): Documentation
    internal class ClientVersionTracker : IEnumerable<(long, WorkerVersion)>
    {
        private ThreadLocalObjectPool<List<long>> listPool = new ThreadLocalObjectPool<List<long>>(() => new List<long>());
        // TODO(Tianyu): Use a more compact representation for encoding completed operation versions
        private Dictionary<WorkerVersion, List<long>> versionMappings = new Dictionary<WorkerVersion, List<long>>();
        // WTF C# cannot remove things from dictionary while iterating?
        private List<WorkerVersion> toRemove = new List<WorkerVersion>();
        private long largestSeqNum = 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long LargestSeqNum() => largestSeqNum;

        internal void Add(long serialNum, WorkerVersion executedAt)
        {
            largestSeqNum = Math.Max(largestSeqNum, serialNum);
            if (!versionMappings.TryGetValue(executedAt, out var list))
            {
                list = listPool.Checkout();
                list.Clear();
                versionMappings.Add(executedAt, list);
            }
            list.Add(serialNum);
        }
        
        internal void ResolveOperations(IDprStateSnapshot dprState)
        {
            foreach (var entry in versionMappings)
            {
                if (dprState.SafeVersion(entry.Key.Worker) >= entry.Key.Version)
                {
                    entry.Value.Clear();
                    listPool.Return(entry.Value);
                    toRemove.Add(entry.Key);
                }
            }

            foreach (var wv in toRemove)
                versionMappings.Remove(wv);
            
            toRemove.Clear();
        }

        internal void HandleRollback(IDprStateSnapshot dprState, ref CommitPoint limit)
        {
            foreach (var entry in versionMappings)
            {
                if (dprState.SafeVersion(entry.Key.Worker) < entry.Key.Version)
                {
                    limit.ExcludedSerialNos.AddRange(entry.Value);
                    listPool.Return(entry.Value);
                    toRemove.Add(entry.Key);
                }
            }
            foreach (var wv in toRemove)
                versionMappings.Remove(wv);
            toRemove.Clear();
        } 

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<(long, WorkerVersion)> GetEnumerator()
        {
            return new ClientVersionTrackerEnumerator(versionMappings);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}