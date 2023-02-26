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
        void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint);
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
        bool DisableEphemeralLocking { get; }

        bool IsManualLocking { get; }

        SessionType SessionType { get; }
        #endregion Optional features supported by this implementation

        #region Reads
        bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo);
        bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo);
        void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason);
        void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason);
        bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo);
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo);
        bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo);
        void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo);
        #endregion InitialUpdater

        #region CopyUpdater
        bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo);
        bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo);
        void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo);
        #endregion CopyUpdater

        #region InPlaceUpdater
        bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo, out OperationStatus status);
        #endregion InPlaceUpdater

        void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        bool SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo);
        void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo);
        bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo);
        #endregion Deletes

        #region Disposal
        void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason);
        void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo);
        void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo);
        void DisposeSingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo);
        void DisposeDeserializedFromDisk(ref Key key, ref Value value, ref RecordInfo recordInfo);
        #endregion Disposal

        #region Ephemeral locking
        bool TryLockEphemeralExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx);
        bool TryLockEphemeralShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx);
        void UnlockEphemeralExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx);
        void UnlockEphemeralShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx);
        #endregion 

        bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

        public FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> Ctx { get; }

        IHeapContainer<Input> GetHeapContainer(ref Input input);
    }
}