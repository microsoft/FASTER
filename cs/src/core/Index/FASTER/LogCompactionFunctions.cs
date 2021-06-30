// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    internal sealed class LogCompactionFunctions<Key, Value, Input, Output, Context, Functions> : IFunctions<Key, Value, Input, Output, Context>
        where Functions : IFunctions<Key, Value, Input, Output, Context>

    {
        readonly Functions _functions;

        public LogCompactionFunctions(Functions functions)
        {
            _functions = functions;
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst)
        { }

        /// <summary>
        /// For compaction, we never perform concurrent writes as rolled over data defers to
        /// newly inserted data for the same key.
        /// </summary>
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) => true;

        public void CopyUpdater(ref Key key, ref Input input, ref Output output, ref Value oldValue, ref Value newValue) { }

        public void DeleteCompletionCallback(ref Key key, Context ctx) { }

        public void InitialUpdater(ref Key key, ref Input input, ref Output output, ref Value value) { }

        public bool InPlaceUpdater(ref Key key, ref Input input, ref Output output, ref Value value) => true;

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue) => true;

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }

        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst) { }

        /// <summary>
        /// Write compacted live value to store
        /// </summary>
        public void SingleWriter(ref Key key, ref Value src, ref Value dst) => _functions.SingleWriter(ref key, ref src, ref dst);

        public void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }

        public bool SupportsLocking => false;
        public void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, ref long lockContext) { }
        public bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, long lockContext) => true;
    }
}