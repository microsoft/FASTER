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
        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address) => true;

        public void SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address) { value = default; }

        public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address) { }

        /// <summary>
        /// No ConcurrentDeleter needed for compaction
        /// </summary>
        public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address) => true;

        /// <summary>
        /// For compaction, we never perform concurrent writes as rolled over data defers to
        /// newly inserted data for the same key.
        /// </summary>
        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address) => true;

        public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address) => true;
        
        public bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address) => true;

        public void DeleteCompletionCallback(ref Key key, Context ctx) { }

        public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address) => true;
        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) { }

        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address) => true;

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output) => true;

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output) => true;

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }

        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address) => true;

        /// <summary>
        /// Write compacted live value to store
        /// </summary>
        public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref int usedLength, int fullLength, long address) 
            => _functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, ref usedLength, fullLength, address);

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address) { }

        public void CopyWriter(ref Key key, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address)
            => _functions.CopyWriter(ref key, ref src, ref dst, ref recordInfo, address);

        public void PostCopyWriter(ref Key key, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address) { }

        public void UpsertCompletionCallback(ref Key key, ref Input input, ref Value value, Context ctx) { }

        public void DisposeKey(ref Key key) { }

        public void DisposeValue(ref Value value) { }
    }
}