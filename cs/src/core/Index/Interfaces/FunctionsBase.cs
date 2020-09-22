// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;

namespace FASTER.core
{
    /// <summary>
    /// Default empty functions base class to make it easy for users to provide
    /// their own implementation
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public abstract class FunctionsBase<Key, Value, Input, Output, Context> : IFunctions<Key, Value, Input, Output, Context>
    {
        public virtual void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref Context ctx) { }
        public virtual void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref Context ctx) { }

        public virtual bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, ref Context ctx) { dst = src; return true; }
        public virtual void SingleWriter(ref Key key, ref Value src, ref Value dst, ref Context ctx) => dst = src;

        public virtual void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Context ctx) { }
        public virtual void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Context ctx) { }
        public virtual bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Context ctx) { return true; }

        public virtual void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }
        public virtual void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status) { }
        public virtual void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        public virtual void DeleteCompletionCallback(ref Key key, Context ctx) { }
        public virtual void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide
    /// their own implementation
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class SimpleFunctions<Key, Value, Context> : FunctionsBase<Key, Value, Value, Value, Context>
    {
        private readonly Func<Value, Value, Value> merger;
        public SimpleFunctions() => merger = (l, r) => l;
        public SimpleFunctions(Func<Value, Value, Value> merger) => this.merger = merger;

        public override void ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref Context ctx) => dst = value;
        public override void SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref Context ctx) => dst = value;

        public override bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, ref Context ctx) { dst = src; return true; }
        public override void SingleWriter(ref Key key, ref Value src, ref Value dst, ref Context ctx) => dst = src;

        public override void InitialUpdater(ref Key key, ref Value input, ref Value value, ref Context ctx) => value = input;
        public override void CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Context ctx) => newValue = merger(input, oldValue);
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Context ctx) { value = merger(input, value); return true; }

        public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status) { }
        public override void RMWCompletionCallback(ref Key key, ref Value input, Context ctx, Status status) { }
        public override void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        public override void DeleteCompletionCallback(ref Key key, Context ctx) { }
        public override void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }

    public class SimpleFunctions<Key, Value> : SimpleFunctions<Key, Value, Empty>
    {
        public SimpleFunctions() : base() { }
        public SimpleFunctions(Func<Value, Value, Value> merger) : base(merger) { }
    }
}