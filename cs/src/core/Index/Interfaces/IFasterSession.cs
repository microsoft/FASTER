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
    /// Provides thread management and all callbacks.
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    internal interface IFasterSession<Key, Value, Input, Output, Context> : IFunctions<Key, Value, Input, Output, Context>, IFasterSession, IVariableLengthStruct<Value, Input>
    {
        // Overloads for locking. Except for readcache/copy-to-tail usage of SingleWriter, all operations that append a record must lock in the <Operation>() call and unlock
        // in the Post<Operation> call; otherwise another session can try to access the record as soon as it's CAS'd and before Post<Operation> is called.
        void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, out long lockContext);
        void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, long lockContext);
        void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address, out long lockContext);
        void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address, long lockContext);
        void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address, out long lockContext);
        bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address, long lockContext);

        bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

        IHeapContainer<Input> GetHeapContainer(ref Input input);
    }
}