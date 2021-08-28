// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.common;

namespace FASTER.client
{
    /// <summary>
    /// Callback functions provided by FASTER client
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class CallbackFunctionsBase<Key, Value, Input, Output, Context> : ICallbackFunctions<Key, Value, Input, Output, Context>
    {
        /// <inheritdoc/>
        public virtual void DeleteCompletionCallback(ref Key key, Context ctx) { }
        /// <inheritdoc/>
        public virtual void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }
        /// <inheritdoc/>
        public virtual void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }
        /// <inheritdoc/>
        public virtual void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        /// <inheritdoc/>
        public virtual void SubscribeKVCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }
        /// <inheritdoc/>
        public virtual void PublishCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        /// <inheritdoc/>
        public virtual void SubscribeCallback(ref Key key, ref Value value, Context ctx) { }
    }
}