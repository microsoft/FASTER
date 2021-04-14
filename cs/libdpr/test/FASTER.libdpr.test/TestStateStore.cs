using System;
using System.Collections.Generic;
using System.Threading;

namespace FASTER.libdpr
{
    // Basic test object
    public class TestStateObject : SimpleStateObject<int>
    {
        private List<(int, int)> opLog = new List<(int, int)>();
        // exclusive
        private int persistedPrefix = 0;

        public void DoStuff((int, int) opId)
        {
            lock (opLog)
            {
                opLog.Add(opId);
            }
        }
        
        protected override void PerformCheckpoint(Action<int> onPersist)
        {
            int prefix;
            lock (opLog)
            {
                prefix = opLog.Count;
            }

            // Simulate I/O by sleeping
            Thread.Sleep(5);

            persistedPrefix = prefix;
            onPersist.Invoke(prefix);
        }

        protected override void RestoreCheckpoint(int token)
        {
            lock (opLog)
            {
                opLog.RemoveRange(token, opLog.Count - token);
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
        public DprServer<TestStateObject, int> dprServer;
        public TestStateObject stateObject;

        public TestStateStore(Worker me, IDprFinder finder)
        {
            stateObject = new TestStateObject();
            dprServer = new DprServer<TestStateObject, int>(finder, me, stateObject);
        }
        
        public void Process(Span<byte> dprHeader, Span<byte> response, (int, int) op)
        {
            if (dprServer.RequestBatchBegin(dprHeader, response, out var tracker))
            {
                var v = stateObject.VersionScheme().Enter();
                stateObject.DoStuff(op);
                tracker.MarkOneOperationVersion(0, v);
                stateObject.VersionScheme().Leave();
                dprServer.SignalBatchFinish(dprHeader, response, tracker);
            }
        }
    }

}