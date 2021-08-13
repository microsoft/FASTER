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
    public interface ICallbackFunctions<Key, Value, Input, Output, Context>
    {
        /// <summary>
        /// Read completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="ctx"></param>
        /// <param name="status"></param>
        void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status);

        /// <summary>
        /// Upsert completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="ctx"></param>
        void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx);

        /// <summary>
        /// RMW completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="ctx"></param>
        /// <param name="status"></param>
        void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status);

        /// <summary>
        /// Delete completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="ctx"></param>
        void DeleteCompletionCallback(ref Key key, Context ctx);

        /// <summary>
        /// Subscribe KV callback
        /// </summary>
        /// <param name="key"></param>
        ///  /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="ctx"></param>
        /// <param name="status"></param>
        void SubscribeKVCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status);
    }
}