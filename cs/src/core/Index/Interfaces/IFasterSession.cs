// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Provides thread management and callback to checkpoint completion (called state machine).
    /// </summary>
    // This is split to two interfaces just to limit infection of <Key, Value, Input, Output, Context> type parameters
    internal interface IFasterSession
    {
        void UnsafeResumeThread();
        void UnsafeSuspendThread();
        void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint);
    }

    /// <summary>
    /// Provides thread management and all callbacks. A wrapper for IFunctions and additional methods called by FasterImpl; the wrapped
    /// IFunctions methods provide additional parameters to support the wrapper functionality, then call through to the user implementations. 
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    internal interface IFasterSession<Key, Value, Input, Output, Context> : IFasterSession, IVariableLengthStruct<Value, Input>
    {
        #region Optional features supported by this implementation
        bool SupportsLocking { get; }

        bool SupportsPostOperations { get; }
        #endregion Optional features supported by this implementation

        #region Reads
        bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref LockOperation lockOp, ref RecordInfo recordInfo, long address);
        bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref LockOperation lockOp, ref RecordInfo recordInfo, long address);
        void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref LockOperation lockOp, ref RecordInfo recordInfo, long address);
        void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref LockOperation lockOp, ref RecordInfo recordInfo, long address);
        bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref LockOperation lockOp, ref RecordInfo recordInfo, long address);
        void UpsertCompletionCallback(ref Key key, ref Input input, ref Value value, Context ctx);
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output);
        void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address, out long lockContext);
        void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address, long lockContext);
        #endregion InitialUpdater

        #region CopyUpdater
        bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output);
        void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address, out long lockContext);
        bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address, long lockContext);
        #endregion CopyUpdater

        #region InPlaceUpdater
        bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address);
        #endregion InPlaceUpdater

        void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address);
        bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address);
        void DeleteCompletionCallback(ref Key key, Context ctx);
        #endregion Deletes

        #region Locking
        void LockExclusive(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext);
        void UnlockExclusive(ref RecordInfo recordInfo, ref Key key, ref Value value, long lockContext);
        bool TryLockExclusive(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext, int spinCount = 1);
        void LockShared(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext);
        bool UnlockShared(ref RecordInfo recordInfo, ref Key key, ref Value value, long lockContext);
        bool TryLockShared(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext, int spinCount = 1);
        void LockExclusiveFromShared(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext);
        bool TryLockExclusiveFromShared(ref RecordInfo recordInfo, ref Key key, ref Value value, ref long lockContext, int spinCount = 1);
        #endregion Locking

        bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

        IHeapContainer<Input> GetHeapContainer(ref Input input);
    }
}