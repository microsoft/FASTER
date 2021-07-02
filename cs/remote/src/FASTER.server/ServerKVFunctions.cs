// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.common;

namespace FASTER.server
{
    internal struct ServerKVFunctions<Key, Value, Input, Output, Functions, ParameterSerializer> : IFunctions<Key, Value, Input, Output, long>
        where Functions : IFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        private readonly Functions functions;
        private readonly FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer> serverNetworkSession;

        public bool SupportsLocking => functions.SupportsLocking;

        public ServerKVFunctions(Functions functions, FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer> serverNetworkSession)
        {
            this.functions = functions;
            this.serverNetworkSession = serverNetworkSession;
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            => functions.CheckpointCompletionCallback(sessionId, commitPoint);

        public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst)
            => functions.ConcurrentReader(ref key, ref input, ref value, ref dst);

        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
            => functions.ConcurrentWriter(ref key, ref src, ref dst);

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output)
            => functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output);

        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output)
            => functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output);

        public void DeleteCompletionCallback(ref Key key, long ctx)
            => functions.DeleteCompletionCallback(ref key, ctx);

        public void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output)
            => functions.InitialUpdater(ref key, ref input, ref value, ref output);

        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output)
            => functions.InPlaceUpdater(ref key, ref input, ref value, ref output);

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, long ctx, Status status)
        {
            serverNetworkSession.CompleteRead(ref output, ctx, status);
            functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status);
        }

        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, long ctx, Status status)
        {
            serverNetworkSession.CompleteRMW(ctx, status);
            functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status);
        }

        public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst)
            => functions.SingleReader(ref key, ref input, ref value, ref dst);

        public void SingleWriter(ref Key key, ref Value src, ref Value dst)
            => functions.SingleWriter(ref key, ref src, ref dst);

        public void UpsertCompletionCallback(ref Key key, ref Value value, long ctx)
            => functions.UpsertCompletionCallback(ref key, ref value, ctx);

        public void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, ref long lockContext)
            => functions.Lock(ref recordInfo, ref key, ref value, lockType, ref lockContext);

        public bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, long lockContext)
            => functions.Unlock(ref recordInfo, ref key, ref value, lockType, lockContext);
    }
}
