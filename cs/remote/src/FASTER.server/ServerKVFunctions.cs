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

        public bool SupportsLocking => functions.SupportsLocking;

        public bool SupportsPostOperations => true;

        public ServerKVFunctions(Functions functions, FasterKVServerSessionBase<Output> serverNetworkSession)
        {
            this.functions = functions;
            this.serverNetworkSession = serverNetworkSession;
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            => functions.CheckpointCompletionCallback(sessionId, commitPoint);

        public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address) { }

        public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address)
            => functions.ConcurrentDeleter(ref key, ref value, ref recordInfo, address);

        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
            => functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref recordInfo, address);

        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address)
            => functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref recordInfo, address);

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output)
            => functions.NeedInitialUpdate(ref key, ref input, ref output);

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output)
            => functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output);

        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
            => functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, address);

        public bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
            => functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, address);

        public void DeleteCompletionCallback(ref Key key, long ctx)
            => functions.DeleteCompletionCallback(ref key, ctx);

        public void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            => functions.InitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);

        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            => functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);

        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) { }

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

        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
            => functions.SingleReader(ref key, ref input, ref value, ref dst, ref recordInfo, address);

        public void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address)
            => functions.SingleWriter(ref key, ref input, ref src, ref dst, ref recordInfo, address);

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address) { }

        public void UpsertCompletionCallback(ref Key key, ref Input input, ref Value value, long ctx)
            => functions.UpsertCompletionCallback(ref key, ref input, ref value, ctx);

        public void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, ref long lockContext)
            => functions.Lock(ref recordInfo, ref key, ref value, lockType, ref lockContext);

        public bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, long lockContext)
            => functions.Unlock(ref recordInfo, ref key, ref value, lockType, lockContext);
    }
}
