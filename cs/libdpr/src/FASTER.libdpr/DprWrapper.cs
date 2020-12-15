using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.core;
using FASTER.serverless;

namespace FASTER.libdpr
{
    class DprServerVersionHandle<Token>
    {
        public Token token;
        public HashSet<WorkerVersion> deps = new HashSet<WorkerVersion>();
    }

    public class DprWrapper<Underlying, Message, Token>
        where Underlying : SessionedStateObject<Message, Token>
        where Message : AppendableMessage
    {
        private Worker me;
        private Underlying stateObject;
        private MetadataStore metadataStore;
        private IBucketingScheme<Message> bucketingScheme;
        private IDprManager dprManager;

        private long currentVersion, currentWorldLine;
        private LightEpoch epoch;

        private Thread syncThread;
        private ManualResetEventSlim termination;
        private ConcurrentDictionary<long, DprServerVersionHandle<Token>> versions;
        private ConcurrentDictionary<Guid, StateObjectSession<Message>> sessions;

        public DprWrapper(Worker me, Underlying stateObject, MetadataStore metadataStore,
            IBucketingScheme<Message> bucketingScheme, IDprManager dprManager)
        {
            this.me = me;
            this.stateObject = stateObject;
            this.metadataStore = metadataStore;
            this.bucketingScheme = bucketingScheme;
            this.dprManager = dprManager;
            currentVersion = 0;
            currentWorldLine = 0;
            epoch = new LightEpoch();
            versions = new ConcurrentDictionary<long, DprServerVersionHandle<Token>>();
            sessions = new ConcurrentDictionary<Guid, StateObjectSession<Message>>();
        }

        public void StartWorker(int checkpointIntervalMilli)
        {
            termination = new ManualResetEventSlim();
            syncThread = new Thread(() =>
            {
                while (!termination.IsSet)
                {
                    dprManager.Refresh();
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

        private StateObjectSession<Message> GetSession(Guid sessionId)
        {
            return sessions.GetOrAdd(sessionId, guid => stateObject.GetSession(guid));
        }

        private unsafe bool TryOperate(ref Message request, ref DprMessage dprRequest, ref Message response,
            ref DprMessage dprResponse)
        {
            if (dprRequest.worldLine != currentWorldLine)
            {
                dprResponse.ret = DprStatus.WORLD_LINE_MISMATCH;
                return true;
            }

            if (dprRequest.version > currentVersion)
            {
                return false;
            }

            var session = GetSession(dprRequest.sessionId);
            epoch.Resume();
            session.Operate(ref request, ref response, dprRequest.serialNum);
            var versionExecuted = session.Version();
            fixed (byte* depValues = dprRequest.deps)
            {
                for (var i = 0; i < dprRequest.numDeps; i++)
                    GetOrAddNewVersion(versionExecuted).deps.Add(Unsafe.AsRef<WorkerVersion>(depValues + sizeof(Guid) * i));
            }
            epoch.Suspend();
            dprResponse.ret = DprStatus.OK;
            // When replying, actually use the precise version for correctness
            dprResponse.version = versionExecuted;
            return true;
        }

        private DprServerVersionHandle<Token> GetOrAddNewVersion(long version)
        {
            return versions.GetOrAdd(version, version => new DprServerVersionHandle<Token>());
        }

        private void BumpVersion(long target)
        {
            var newVersion = stateObject.BeginCheckpoint(w =>
            {
                var versionObject = versions[w.Item1];
                versionObject.token = w.Item2;
                var workerVersion = new WorkerVersion(me, w.Item1);
                epoch.BumpCurrentEpoch(() => dprManager.ReportNewPersistentVersion(workerVersion, versionObject.deps));
            }, target);
            Debug.Assert(newVersion >= target);

            if (Utility.MonotonicUpdate(ref currentVersion, newVersion, out _))
                GetOrAddNewVersion(newVersion);
        }

        public unsafe void Operate(ref Message request, ref Message response)
        {
            fixed (byte* m = request.GetAdditionalBytes())
            {
                ref var dprRequest = ref Unsafe.AsRef<DprMessage>(m);
                var dprResponse = dprRequest;
                dprResponse.numDeps = 0;

                if (metadataStore.ValidateLocalOwnership(bucketingScheme.GetBucket(request)))
                {
                    dprResponse.ret = DprStatus.OWNERSHIP_MISMATCH;
                    response.AddBytes(new Span<byte>(dprResponse.data, DprMessage.HeaderSize));
                    return;
                }

                while (!TryOperate(ref request, ref dprRequest, ref response, ref dprResponse))
                {
                    BumpVersion(dprRequest.version);
                }

                response.AddBytes(new Span<byte>(dprResponse.data, DprMessage.HeaderSize));
            }
        }
    }
}