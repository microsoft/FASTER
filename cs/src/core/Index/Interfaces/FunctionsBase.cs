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
        protected readonly bool locking;

        protected FunctionsBase(bool locking = false) => this.locking = locking;

        /// <inheritdoc/>
        public virtual void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst) { }
        /// <inheritdoc/>
        public virtual void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst) { }

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { dst = src; return true; }
        /// <inheritdoc/>
        public virtual void SingleWriter(ref Key key, ref Value src, ref Value dst) => dst = src;

        /// <inheritdoc/>
        public virtual void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output) { }
        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output) => true;
        /// <inheritdoc/>
        public virtual void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output) { }
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output) => true;

        /// <inheritdoc/>
        public virtual void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }
        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }
        /// <inheritdoc/>
        public virtual void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        /// <inheritdoc/>
        public virtual void DeleteCompletionCallback(ref Key key, Context ctx) { }
        /// <inheritdoc/>
        public virtual void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }

        /// <inheritdoc/>
        public virtual bool SupportsLocking => locking;

        /// <inheritdoc/>
        public virtual void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, ref long lockContext)
        {
            recordInfo.SpinLock();
        }

        /// <inheritdoc/>
        public virtual bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, long lockContext)
        {
            recordInfo.Unlock();
            return true;
        }
    }

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of FunctionsBase
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class SimpleFunctions<Key, Value, Context> : FunctionsBase<Key, Value, Value, Value, Context>
    {
        public SimpleFunctions(bool locking = false) : base(locking) { }

        private readonly Func<Value, Value, Value> merger;
        public SimpleFunctions() => merger = (l, r) => l;
        public SimpleFunctions(Func<Value, Value, Value> merger) => this.merger = merger;

        /// <inheritdoc/>
        public override void ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst) => dst = value;
        /// <inheritdoc/>
        public override void SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst) => dst = value;

        /// <inheritdoc/>
        public override bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { dst = src; return true; }
        /// <inheritdoc/>
        public override void SingleWriter(ref Key key, ref Value src, ref Value dst) => dst = src;

        /// <inheritdoc/>
        public override void InitialUpdater(ref Key key, ref Value input, ref Value value, ref Value output) => value = input;
        /// <inheritdoc/>
        public override void CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Value output) => newValue = merger(input, oldValue);
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output) { value = merger(input, value); return true; }

        /// <inheritdoc/>
        public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status) { }
        /// <inheritdoc/>
        public override void RMWCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status) { }
        /// <inheritdoc/>
        public override void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
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

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of IAdvancedFunctions
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public abstract class AdvancedFunctionsBase<Key, Value, Input, Output, Context> : IAdvancedFunctions<Key, Value, Input, Output, Context>
    {
        protected readonly bool locking;

        protected AdvancedFunctionsBase(bool locking = false) => this.locking = locking;

        /// <inheritdoc/>
        public virtual void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address) { }
        /// <inheritdoc/>
        public virtual void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, long address) { }

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address) { dst = src; return true; }
        /// <inheritdoc/>
        public virtual void SingleWriter(ref Key key, ref Value src, ref Value dst) => dst = src;

        /// <inheritdoc/>
        public virtual void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output) { }
        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output) => true;
        /// <inheritdoc/>
        public virtual void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output) { }
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) => true;

        /// <inheritdoc/>
        public virtual void ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address) { }

        /// <inheritdoc/>
        public virtual void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo) { }
        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }
        /// <inheritdoc/>
        public virtual void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        /// <inheritdoc/>
        public virtual void DeleteCompletionCallback(ref Key key, Context ctx) { }
        /// <inheritdoc/>
        public virtual void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }

        /// <inheritdoc/>
        public virtual bool SupportsLocking => locking;

        /// <inheritdoc/>
        public virtual void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, ref long lockContext)
        {
            recordInfo.SpinLock();
        }

        /// <inheritdoc/>
        public virtual bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, long lockContext)
        {
            recordInfo.Unlock();
            return true;
        }
    }

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of AdvancedFunctionsBase
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class AdvancedSimpleFunctions<Key, Value, Context> : AdvancedFunctionsBase<Key, Value, Value, Value, Context>
    {
        public AdvancedSimpleFunctions(bool locking = false) : base(locking) { }

        private readonly Func<Value, Value, Value> merger;
        public AdvancedSimpleFunctions() => merger = (l, r) => l;
        public AdvancedSimpleFunctions(Func<Value, Value, Value> merger) => this.merger = merger;

        /// <inheritdoc/>
        public override void ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref RecordInfo recordInfo, long address) => dst = value;
        /// <inheritdoc/>
        public override void SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst, long address) => dst = value;

        /// <inheritdoc/>
        public override bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address) { dst = src; return true; }
        /// <inheritdoc/>
        public override void SingleWriter(ref Key key, ref Value src, ref Value dst) => dst = src;

        /// <inheritdoc/>
        public override void InitialUpdater(ref Key key, ref Value input, ref Value value, ref Value output) => value = input;
        /// <inheritdoc/>
        public override void CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Value output) => newValue = merger(input, oldValue);
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RecordInfo recordInfo, long address) { value = merger(input, value); return true; }

        /// <inheritdoc/>
        public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status, RecordInfo recordInfo) { }
        /// <inheritdoc/>
        public override void RMWCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status) { }
        /// <inheritdoc/>
        public override void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        /// <inheritdoc/>
        public override void DeleteCompletionCallback(ref Key key, Context ctx) { }
        /// <inheritdoc/>
        public override void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }

    public class AdvancedSimpleFunctions<Key, Value> : AdvancedSimpleFunctions<Key, Value, Empty>
    {
        public AdvancedSimpleFunctions() : base() { }
        public AdvancedSimpleFunctions(Func<Value, Value, Value> merger) : base(merger) { }
    }
}