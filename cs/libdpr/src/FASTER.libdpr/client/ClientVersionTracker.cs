using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using FASTER.core;

namespace FASTER.libdpr
{
    public class ClientVersionTrackerEnumerator : IEnumerator<(long, WorkerVersion)>
    {
        private IEnumerator<KeyValuePair<WorkerVersion, List<long>>> versionEnumerator;
        private IEnumerator<long> opEnumerator;

        public ClientVersionTrackerEnumerator(Dictionary<WorkerVersion, List<long>> dict)
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
    
    public class ClientVersionTracker : IEnumerable<(long, WorkerVersion)>
    {
        private ThreadLocalObjectPool<List<long>> listPool = new ThreadLocalObjectPool<List<long>>(() => new List<long>());
        // TODO(Tianyu): Use a more compact representation for encoding completed operation versions
        private Dictionary<WorkerVersion, List<long>> exceptionMappings = new Dictionary<WorkerVersion, List<long>>();
        // WTF C# cannot remove things from dictionary while iterating?
        private List<WorkerVersion> toRemove = new List<WorkerVersion>();
        private long largestSeqNum = 0;

        // TODO(Tianyu): Use a more compact representation for encoding completed operation versions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]

        public long LargestSeqNum() => largestSeqNum;

        public void Add(long serialNum, WorkerVersion executedAt)
        {
            largestSeqNum = Math.Max(largestSeqNum, serialNum);
            if (!exceptionMappings.TryGetValue(executedAt, out var list))
            {
                list = listPool.Checkout();
                list.Clear();
                exceptionMappings.Add(executedAt, list);
            }
            list.Add(serialNum);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool PendingOperationRecoverable(ref WorkerVersion executedAt, IDprTableSnapshot dprTable)
        {
            return executedAt.Version != -1 && executedAt.Version <= dprTable.SafeVersion(executedAt.Worker);
        }

        public void ResolveOperations(IDprTableSnapshot dprTable)
        {
            foreach (var entry in exceptionMappings)
            {
                if (dprTable.SafeVersion(entry.Key.Worker) >= entry.Key.Version)
                {
                    entry.Value.Clear();
                    listPool.Return(entry.Value);
                    toRemove.Add(entry.Key);
                }
            }

            foreach (var wv in toRemove)
                exceptionMappings.Remove(wv);
            
            toRemove.Clear();
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool OperationRecovered(ref CommitPoint recoveredProgress, long sequenceNumber)
        {
            // TODO(Tianyu): Currently, because remote workers do not return results that are pending, the only
            // exceptions in recovered progress will be locally pending operations, which is disjoint from the
            // exceptions tracked in this class. Therefore there is no need to check the exception list here.
            return sequenceNumber < recoveredProgress.UntilSerialNo;
        }

        public void DropRolledbackExceptions(ref CommitPoint recoveredProgress)
        {
           // TODO(Tianyu): Implement more fine-grained later, will probably be easier if recovery comes with the exact version cut off
           // for each worker.
           foreach (var entry in exceptionMappings)
           {
               entry.Value.Clear();
               listPool.Return(entry.Value);
           }
           exceptionMappings.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<(long, WorkerVersion)> GetEnumerator()
        {
            return new ClientVersionTrackerEnumerator(exceptionMappings);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}