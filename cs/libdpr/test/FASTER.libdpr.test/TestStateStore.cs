using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.libdpr
{
    // Basic test object
    public class TestStateObject : SimpleStateObject
    {
        private List<(int, int)> opLog = new List<(int, int)>();
        // exclusive
        private int persistedPrefix = 0;
        private Dictionary<long, int> prefixes = new Dictionary<long, int>();

        public void DoStuff((int, int) opId)
        {
            lock (opLog)
            {
                opLog.Add(opId);
            }
        }
        
        protected override void PerformCheckpoint(long version, Action onPersist)
        {
            int prefix;
            lock (opLog)
            {
                prefix = opLog.Count;
            }

            // Simulate I/O by sleeping
            Thread.Sleep(5);

            prefixes[version] = persistedPrefix = prefix;
            onPersist.Invoke();
        }

        protected override void RestoreCheckpoint(long version)
        {
            lock (opLog)
            {
                opLog.RemoveRange(prefixes[version], opLog.Count - prefixes[version]);
            }
        }

        public HashSet<(int, int)> GetOpsPersisted()
        {
            lock (opLog)
            {
                return new HashSet<(int, int)>(opLog.GetRange(0, persistedPrefix));
            }
        }
    }
    
    public class TestStateStore
    {
        public DprServer<TestStateObject> dprServer;
        public TestStateObject stateObject;

        public TestStateStore(Worker me, IDprFinder finder)
        {
            stateObject = new TestStateObject();
            dprServer = new DprServer<TestStateObject>(finder, me, stateObject);
        }
        
        public void Process(Span<byte> dprHeader, Span<byte> response, (int, int) op)
        {
            ref var dprRequest =
                ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchRequestHeader>(response));
            ref var dprResponse = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchResponseHeader>(response));

            if (dprServer.RequestBatchBegin(ref dprRequest, ref dprResponse, out var tracker))
            {
                var v = stateObject.VersionScheme().Enter();
                stateObject.DoStuff(op);
                tracker.MarkOneOperationVersion(0, v);
                stateObject.VersionScheme().Leave();
                dprServer.SignalBatchFinish(ref dprRequest, response, tracker);
            }
        }
    }

}