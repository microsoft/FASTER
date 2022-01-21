// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;

namespace FASTER.core
{
    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of IFunctions
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public abstract class FunctionsBase<Key, Value, Input, Output, Context> : IFunctions<Key, Value, Input, Output, Context>
    {
        /// <inheritdoc/>
        public virtual bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address) => true;
        /// <inheritdoc/>
        public virtual bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address) => true;

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address) { dst = src; return true; }
        /// <inheritdoc/>
        public virtual void SingleWriter(WriteReason reason, ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address) => dst = src;
        /// <inheritdoc/>
        public virtual void PostSingleWriter(WriteReason reason, ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address) { }

        /// <inheritdoc/>
        public virtual void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) { }
        /// <inheritdoc/>
        public virtual void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) { }
        /// <inheritdoc/>
        public virtual bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output) => true;
        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output) => true;
        /// <inheritdoc/>
        public virtual void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address) { }
        /// <inheritdoc/>
        public virtual bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address) => true;
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) => true;

        /// <inheritdoc/>
        public virtual void SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address) { value = default; }
        public virtual void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address) { }
        public virtual bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address) => true;

        public virtual void DisposeKey(ref Key key) { }
        public virtual void DisposeValue(ref Value value) { }

        /// <inheritdoc/>
        public virtual void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public virtual void UpsertCompletionCallback(ref Key key, ref Input input, ref Value value, Context ctx) { }
        /// <inheritdoc/>
        public virtual void DeleteCompletionCallback(ref Key key, Context ctx) { }
        /// <inheritdoc/>
        public virtual void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of FunctionsBase
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class SimpleFunctions<Key, Value, Context> : FunctionsBase<Key, Value, Value, Value, Context>
    {
        private readonly Func<Value, Value, Value> merger;
        public SimpleFunctions() => merger = (l, r) => l;
        public SimpleFunctions(Func<Value, Value, Value> merger) => this.merger = merger;

        /// <inheritdoc/>
        public override bool ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref RecordInfo recordInfo, long address)
        {
            dst = value;
            return true;
        }

        /// <inheritdoc/>
        public override bool SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref RecordInfo recordInfo, long address)
        {
            dst = value;
            return true;
        }

        /// <inheritdoc/>
        public override bool ConcurrentWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref Value output, ref RecordInfo recordInfo, long address) { dst = src; return true; }
        /// <inheritdoc/>
        public override void SingleWriter(WriteReason reason, ref Key key, ref Value input, ref Value src, ref Value dst, ref Value output, ref RecordInfo recordInfo, long address) => dst = src;

        /// <inheritdoc/>
        public override void InitialUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RecordInfo recordInfo, long address) => value = input;
        /// <inheritdoc/>
        public override void CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Value output, ref RecordInfo recordInfo, long address) => newValue = merger(input, oldValue);

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RecordInfo recordInfo, long address) { value = merger(input, value); return true; }

        /// <inheritdoc/>
        public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public override void RMWCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public override void UpsertCompletionCallback(ref Key key, ref Value input, ref Value value, Context ctx) { }
        /// <inheritdoc/>
        public override void DeleteCompletionCallback(ref Key key, Context ctx) { }
        /// <inheritdoc/>
        public override void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }

    public class SimpleFunctions<Key, Value> : SimpleFunctions<Key, Value, Empty>
    {
        public SimpleFunctions() : base() { }
        public SimpleFunctions(Func<Value, Value, Value> merger) : base(merger) { }
    }
}