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
        public virtual bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo) => true;
        /// <inheritdoc/>
        public virtual bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo) => true;

        /// <inheritdoc/>
        public virtual bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { dst = src; return true; }
        /// <inheritdoc/>
        public virtual void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, WriteReason reason) => dst = src;
        /// <inheritdoc/>
        public virtual void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, WriteReason reason) { }

        /// <inheritdoc/>
        public virtual void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }
        /// <inheritdoc/>
        public virtual void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }
        /// <inheritdoc/>
        public virtual bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref UpdateInfo updateInfo) => true;
        /// <inheritdoc/>
        public virtual bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref UpdateInfo updateInfo) => true;
        /// <inheritdoc/>
        public virtual void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }
        /// <inheritdoc/>
        public virtual bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => true;
        /// <inheritdoc/>
        public virtual bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => true;

        /// <inheritdoc/>
        public virtual void SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { value = default; }
        public virtual void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { }
        public virtual bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => true;

        public virtual void DisposeKey(ref Key key) { }
        public virtual void DisposeValue(ref Value value) { }

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
        public override bool ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            dst = value;
            return true;
        }

        /// <inheritdoc/>
        public override bool SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            dst = value;
            return true;
        }

        /// <inheritdoc/>
        public override void InitialUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => value = input;
        /// <inheritdoc/>
        public override void CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Value output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => newValue = merger(input, oldValue);
        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { value = merger(input, value); return true; }
    }

    public class SimpleFunctions<Key, Value> : SimpleFunctions<Key, Value, Empty>
    {
        public SimpleFunctions() : base() { }
        public SimpleFunctions(Func<Value, Value, Value> merger) : base(merger) { }
    }
}