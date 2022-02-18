// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using FASTER.core;

namespace AsyncStress
{
    public class FasterWrapper<Key, Value> : IFasterWrapper<Key, Value>
    {
        readonly FasterKV<Key, Value> _store;
        readonly AsyncPool<ClientSession<Key, Value, Value, Value, Empty, SimpleFunctions<Key, Value, Empty>>> _sessionPool;
        readonly bool useOsReadBuffering;
        int pendingCount = 0;

        // This can be used to verify the same amount data is loaded.
        public long TailAddress => _store.Log.TailAddress;

        // Indicates how many upsert operations went pending
        public int PendingCount { get => pendingCount; set => pendingCount = value; }

        public void ClearPendingCount() => pendingCount = 0;

        // Whether OS Read buffering is enabled
        public bool UseOsReadBuffering => useOsReadBuffering;

        public FasterWrapper(bool isRefType, bool useLargeLog, bool useOsReadBuffering = false)
        {
            var logDirectory = "d:/AsyncStress";
            var logFileName = Guid.NewGuid().ToString();
            var logSettings = new LogSettings
            {
                LogDevice = new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true, osReadBuffering: useOsReadBuffering),
                ObjectLogDevice = isRefType ? new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.obj.log"), deleteOnClose: true, osReadBuffering: useOsReadBuffering) : default
            };

            if (!useLargeLog)
            {
                logSettings.PageSizeBits = 12;
                logSettings.MemorySizeBits = 13;
            }

            Console.WriteLine($"    FasterWrapper using {logSettings.LogDevice.GetType()}, {(useLargeLog ? "large" : "small")} memory log, and {(isRefType ? string.Empty : "no ")}object log");

            this.useOsReadBuffering = useOsReadBuffering;
            _store = new FasterKV<Key, Value>(1L << 20, logSettings);
            _sessionPool = new AsyncPool<ClientSession<Key, Value, Value, Value, Empty, SimpleFunctions<Key, Value, Empty>>>(
                    logSettings.LogDevice.ThrottleLimit,
                    () => _store.For(new SimpleFunctions<Key, Value, Empty>()).NewSession<SimpleFunctions<Key, Value, Empty>>());
        }

        async ValueTask<int> CompleteAsync(FasterKV<Key, Value>.UpsertAsyncResult<Value, Value, Empty> result)
        {
            var numPending = 0;
            for (; result.Status.Pending; ++numPending)
                result = await result.CompleteAsync().ConfigureAwait(false);
            return numPending;
        }

        async ValueTask<int> CompleteAsync(FasterKV<Key, Value>.RmwAsyncResult<Value, Value, Empty> result)
        {
            var numPending = 0;
            for (; result.Status.Pending; ++numPending)
                result = await result.CompleteAsync().ConfigureAwait(false);
            return numPending;
        }

        public async ValueTask UpsertAsync(Key key, Value value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync().ConfigureAwait(false);
            Interlocked.Add(ref pendingCount, await CompleteAsync(await session.UpsertAsync(key, value).ConfigureAwait(false)));
            _sessionPool.Return(session);
        }

        public void Upsert(Key key, Value value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();
            var status = session.Upsert(key, value);
            Assert.False(status.Pending);
            _sessionPool.Return(session);
        }

        public async ValueTask UpsertChunkAsync((Key, Value)[] chunk, int offset, int count)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync().ConfigureAwait(false);

            for (var i = 0; i < count; ++i)
                Interlocked.Add(ref pendingCount, await CompleteAsync(await session.UpsertAsync(chunk[offset + i].Item1, chunk[offset + i].Item2)));
            _sessionPool.Return(session);
        }

        public async ValueTask RMWAsync(Key key, Value value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync().ConfigureAwait(false);
            Interlocked.Add(ref pendingCount, await CompleteAsync(await session.RMWAsync(key, value).ConfigureAwait(false)));
            _sessionPool.Return(session);
        }

        public void RMW(Key key, Value value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();
            var status = session.RMW(key, value);
            Assert.False(status.Pending);
            _sessionPool.Return(session);
        }

        public async ValueTask RMWChunkAsync((Key, Value)[] chunk, int offset, int count)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync().ConfigureAwait(false);

            for (var i = 0; i < count; ++i)
                Interlocked.Add(ref pendingCount, await CompleteAsync(await session.RMWAsync(chunk[offset + i].Item1, chunk[offset + i].Item2)));
            _sessionPool.Return(session);
        }

        public async ValueTask<(Status, Value)> ReadAsync(Key key)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync().ConfigureAwait(false);
            var result = (await session.ReadAsync(key).ConfigureAwait(false)).Complete();
            _sessionPool.Return(session);
            return result;
        }

        public ValueTask<(Status, Value)> Read(Key key)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();
            var result = session.Read(key);
            if (result.status.Pending)
            {
                session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                int count = 0;
                for (; completedOutputs.Next(); ++count)
                {
                    Assert.Equal(key, completedOutputs.Current.Key);
                    result = (completedOutputs.Current.Status, completedOutputs.Current.Output);
                }
                completedOutputs.Dispose();
                Assert.Equal(1, count);
            }
            _sessionPool.Return(session);
            return new ValueTask<(Status, Value)>(result);
        }

        public async ValueTask<(Status, Value)[]> ReadChunkAsync(Key[] chunk, int offset, int count)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();

            // Reads in chunk are performed serially
            (Status, Value)[] result = new (Status, Value)[count];
            for (var i = 0; i < count; ++i)
                result[i] = (await session.ReadAsync(chunk[offset + i]).ConfigureAwait(false)).Complete();

            _sessionPool.Return(session);
            return result;
        }

        public void Dispose()
        {
            _sessionPool.Dispose();
            _store.Dispose();
        }
    }
}
