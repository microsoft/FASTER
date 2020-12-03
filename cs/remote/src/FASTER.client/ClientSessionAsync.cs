﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using FASTER.common;

namespace FASTER.client
{
    public partial class ClientSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer> : IDisposable
            where Functions : ICallbackFunctions<Key, Value, Input, Output, Context>
            where ParameterSerializer : IClientSerializer<Key, Value, Input, Output>
    {
        public async Task<(Status, Output)> ReadAsync(Key key, Input input = default, bool forceFlush = true)
        {
            var tcs = new TaskCompletionSource<(Status, Output)>(TaskCreationOptions.RunContinuationsAsynchronously);
            Output output = default;
            InternalRead(MessageType.ReadAsync, ref key, ref input, ref output);
            tcsQueue.Enqueue(tcs);
            if (forceFlush) Flush();
            return await tcs.Task;
        }

        public async Task UpsertAsync(Key key, Value value, bool forceFlush = true)
        {
            var tcs = new TaskCompletionSource<(Status, Output)>(TaskCreationOptions.RunContinuationsAsynchronously);
            InternalUpsert(MessageType.UpsertAsync, ref key, ref value);
            tcsQueue.Enqueue(tcs);
            if (forceFlush) Flush();
            _ = await tcs.Task;
            return;
        }

        public async Task<Status> RMWAsync(Key key, Input input, bool forceFlush = true)
        {
            var tcs = new TaskCompletionSource<(Status, Output)>(TaskCreationOptions.RunContinuationsAsynchronously);
            InternalRMW(MessageType.RMWAsync, ref key, ref input);
            tcsQueue.Enqueue(tcs);
            if (forceFlush) Flush();
            (var status, _) = await tcs.Task;
            return status;
        }

        public async Task DeleteAsync(Key key, bool forceFlush = true)
        {
            var tcs = new TaskCompletionSource<(Status, Output)>(TaskCreationOptions.RunContinuationsAsynchronously);
            InternalDelete(MessageType.DeleteAsync, ref key);
            tcsQueue.Enqueue(tcs);
            if (forceFlush) Flush();
            _ = await tcs.Task;
            return;
        }
    }
}