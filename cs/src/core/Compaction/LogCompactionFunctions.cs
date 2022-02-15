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

        public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint) { }

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo) => true;

        public void SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { value = default; }

        public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }

        /// <summary>
        /// No ConcurrentDeleter needed for compaction
        /// </summary>
        public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => true;

        /// <summary>
        /// For compaction, we never perform concurrent writes as rolled over data defers to
        /// newly inserted data for the same key.
        /// </summary>
        public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => true;

        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }
        
        public bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => true;

        public void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }
        public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }

        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => true;

        public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref UpdateInfo updateInfo) => true;

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref UpdateInfo updateInfo) => true;

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }

        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }

        /// <summary>
        /// No reads during compaction
        /// </summary>
        public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo) => true;

        /// <summary>
        /// Write compacted live value to store
        /// </summary>
        public void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, WriteReason reason) 
            => _functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, ref updateInfo, reason);

        public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, WriteReason reason) { }

        public void DisposeKey(ref Key key) { }

        public void DisposeValue(ref Value value) { }
    }
}