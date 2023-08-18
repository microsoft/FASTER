// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Threading.Tasks;

namespace AsyncStress
{
    public interface IFasterWrapper<Key, Value>
    {
        long TailAddress { get; }
        int PendingCount { get; set; }
        void ClearPendingCount();
        bool UseOsReadBuffering { get; }

        void Dispose();

        ValueTask<(Status, Value)> Read(Key key);
        ValueTask<(Status, Value)> ReadAsync(Key key);
        ValueTask<(Status, Value)[]> ReadChunkAsync(Key[] chunk, int offset, int count);

        void Upsert(Key key, Value value);
        ValueTask UpsertAsync(Key key, Value value);
        ValueTask UpsertChunkAsync((Key, Value)[] chunk, int offset, int count);

        void RMW(Key key, Value value);
        ValueTask RMWAsync(Key key, Value value);
        ValueTask RMWChunkAsync((Key, Value)[] chunk, int offset, int count);
    }
}