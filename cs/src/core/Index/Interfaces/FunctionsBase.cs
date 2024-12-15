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
        public virtual bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo) => true;
        /// <inheritdoc/>
        public virtual bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo) => true;

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo) { dst = src; return true; }
        /// <inheritdoc/>
        public virtual bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { dst = src; return true; }
        /// <inheritdoc/>
        public virtual void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }

        /// <inheritdoc/>
        public virtual bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }
        /// <inheritdoc/>
        public virtual bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) => true;
        /// <inheritdoc/>
        public virtual void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) => true;

        /// <inheritdoc/>
        public virtual bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) { value = default; return true; }
        public virtual void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo) { }
        public virtual bool ConcurrentDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) => true;

        /// <inheritdoc/>
        public virtual void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }
        /// <inheritdoc/>
        public virtual void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }
        /// <inheritdoc/>
        public virtual void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }
        /// <inheritdoc/>
        public virtual void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) { }
        /// <inheritdoc/>
        public virtual void DisposeDeserializedFromDisk(ref Key key, ref Value value) { }

        /// <inheritdoc/>
        public virtual void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) { }
        /// <inheritdoc/>
        public virtual void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint) { }
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
        public override bool ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref ReadInfo readInfo)
        {
            dst = value;
            return true;
        }

        /// <inheritdoc/>
        public override bool SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref ReadInfo readInfo)
        {
            dst = value;
            return true;
        }

        public override bool SingleWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref Value output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            var result = base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
            if (result)
                output = dst;
            return result;
        }

        public override bool ConcurrentWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref Value output, ref UpsertInfo upsertInfo)
        {
            var result = base.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo);
            if (result)
                output = dst;
            return result;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RMWInfo rmwInfo) { value = output = input; return true; }
        /// <inheritdoc/>
        public override bool CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Value output, ref RMWInfo rmwInfo) { newValue = output = merger(input, oldValue); return true; }
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RMWInfo rmwInfo) { value = output = merger(input, value); return true; }
    }

    public class SimpleFunctions<Key, Value> : SimpleFunctions<Key, Value, Empty>
    {
        public SimpleFunctions() : base() { }
        public SimpleFunctions(Func<Value, Value, Value> merger) : base(merger) { }
    }
}