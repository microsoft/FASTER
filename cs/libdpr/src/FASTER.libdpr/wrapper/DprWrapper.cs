using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    public class DprWrapper<TStateObject, TSession, TBatch, TMessage, TToken>
        where TStateObject : IStateObject<TSession, TBatch, TMessage, TToken>
        where TSession : IStateObjectSession<TBatch, TMessage>
        where TBatch : IAppendableMessageBatch<TMessage>
    {
        private class DprServerVersionHandle<Token>
        {
            public Token token;
            public HashSet<WorkerVersion> deps = new HashSet<WorkerVersion>();
        }
        
        private Worker me;
        private TStateObject stateObject;
        private IDprFinder dprFinder;
    
        private long currentVersion, currentWorldLine;
        private LightEpoch epoch;
    
        private Thread syncThread;
        private ManualResetEventSlim termination;
        private ConcurrentDictionary<long, DprServerVersionHandle<TToken>> versions;
        private ConcurrentDictionary<Guid, IStateObjectSession<TBatch, TMessage>> sessions;
    
        public DprWrapper(Worker me, TStateObject stateObject, IDprFinder dprFinder)
        {
            this.me = me;
            this.stateObject = stateObject;
            this.dprFinder = dprFinder;
            currentVersion = 0;
            currentWorldLine = 0;
            epoch = new LightEpoch();
            versions = new ConcurrentDictionary<long, DprServerVersionHandle<TToken>>();
            sessions = new ConcurrentDictionary<Guid, IStateObjectSession<TBatch, TMessage>>();
        }
    
        public void StartWorker(int checkpointIntervalMilli)
        {
            termination = new ManualResetEventSlim();
            syncThread = new Thread(() =>
            {
                while (!termination.IsSet)
                {
                    dprFinder.Refresh();
                    Thread.Sleep(checkpointIntervalMilli);
                    BumpVersion(currentVersion + 1);
                }
            });
        }
    
        public void StopWorker()
        {
            termination.Set();
            syncThread.Join();
        }
    
        public unsafe void ProcessBatch(ref TBatch request, ref TBatch response)
        {
            fixed (byte* m = request.GetAdditionalBytes())
            {
                ref var dprRequest = ref Unsafe.AsRef<DprBatchHeader>(m);
                // TODO(Tianyu): Unsafe memory access, replace with a buffer from some reusable pool
                var dprResponse = dprRequest;
                dprResponse.numDeps = 0;
    
                while (!TryProcessBatch(ref request, ref dprRequest, ref response, ref dprResponse))
                {
                    BumpVersion(dprRequest.versionLowerBound);
                }
    
                response.AddBytes(new Span<byte>(dprResponse.data, dprResponse.size));
            }
        }
    
        private IStateObjectSession<TBatch, TMessage> GetSession(Guid sessionId)
        {
            return sessions.GetOrAdd(sessionId, guid => stateObject.GetSession(guid));
        }
    
        private unsafe bool TryProcessBatch(ref TBatch request, ref DprBatchHeader dprRequest, ref TBatch response,
            ref DprBatchHeader dprResponse)
        {
            if (dprRequest.worldLine < currentWorldLine)
            {
                dprResponse.worldLine = currentWorldLine;
                dprResponse.size = DprBatchHeader.HeaderSize;
                dprResponse.numMessages = 0;
                dprResponse.numDeps = 0;
                return true;
            }
    
            if (dprRequest.versionLowerBound > currentVersion) return false;
    
            var session = GetSession(dprRequest.sessionId);
            epoch.Resume();
            fixed (byte* depValues = dprRequest.deps)
            {
                for (var i = 0; i < dprRequest.numDeps; i++)
                    GetOrAddNewVersion(currentVersion).deps.Add(Unsafe.AsRef<WorkerVersion>(depValues + sizeof(Guid) * i));
            }
            session.ProcessBatch(ref request, ref response, ref dprResponse);
            epoch.Suspend();
            return true;
        }
    
        private DprServerVersionHandle<TToken> GetOrAddNewVersion(long version)
        {
            return versions.GetOrAdd(version, version => new DprServerVersionHandle<TToken>());
        }
    
       
    }
}