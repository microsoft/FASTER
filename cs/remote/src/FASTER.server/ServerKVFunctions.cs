// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.server
{
    internal struct ServerKVFunctions<Key, Value, Input, Output, Functions> : IFunctions<Key, Value, Input, Output, long>
        where Functions : IFunctions<Key, Value, Input, Output, long>
    {
        private readonly Functions functions;
        private readonly FasterKVServerSessionBase<Output> serverNetworkSession;

        public ServerKVFunctions(Functions functions, FasterKVServerSessionBase<Output> serverNetworkSession)
        {
            this.functions = functions;
            this.serverNetworkSession = serverNetworkSession;
        }

        public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
            => functions.CheckpointCompletionCallback(sessionID, sessionName, commitPoint);

        public bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo)
            => functions.SingleDeleter(ref key, ref value, ref deleteInfo);

        public void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo) { }

        public bool ConcurrentDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo)
            => functions.ConcurrentDeleter(ref key, ref value, ref deleteInfo);

        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
            => functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo);

        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo)
            => functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo);

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            => functions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo)
            => functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output, ref rmwInfo);

        public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo)
            => functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);

        public void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo)
            => functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);

        public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
            => functions.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);

        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
            => functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);

        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, long ctx, Status status, RecordMetadata recordMetadata)
        {
            serverNetworkSession.CompleteRead(ref output, ctx, status);
            functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);
        }

        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, long ctx, Status status, RecordMetadata recordMetadata)
        {
            serverNetworkSession.CompleteRMW(ref output, ctx, status);
            functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);
        }

        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
            => functions.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

        public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) 
            => functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        public void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
            => functions.DisposeSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);

        public void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo)
            => functions.DisposeCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);

        public void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
            => DisposeInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);

        public void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo)
            => functions.DisposeSingleDeleter(ref key, ref value, ref deleteInfo);

        public void DisposeDeserializedFromDisk(ref Key key, ref Value value)
            => functions.DisposeDeserializedFromDisk(ref key, ref value);

        public void DisposeForRevivification(ref Key key, ref Value value, int newKeyLength)
            => functions.DisposeForRevivification(ref key, ref value, newKeyLength);
    }
}
