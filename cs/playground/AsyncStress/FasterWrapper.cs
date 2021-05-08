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
        int upsertPendingCount = 0;

        // This can be used to verify the same amount data is loaded.
        public long TailAddress => _store.Log.TailAddress;

        // Indicates how many upsert operations went pending
        public int UpsertPendingCount { get => upsertPendingCount; set => upsertPendingCount = value; }
        // Whether OS Read buffering is enabled
        public bool UseOsReadBuffering => useOsReadBuffering;

        public FasterWrapper(bool useOsReadBuffering = false)
        {
            var logDirectory = "d:/AsyncStress";
            var logFileName = Guid.NewGuid().ToString();
            var logSettings = new LogSettings
            {
                LogDevice = new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true, osReadBuffering: useOsReadBuffering),
                ObjectLogDevice = new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.obj.log"), deleteOnClose: true, osReadBuffering: useOsReadBuffering),
                PageSizeBits = 12,
                MemorySizeBits = 13
            };

            Console.WriteLine($"    Using {logSettings.LogDevice.GetType()}");

            this.useOsReadBuffering = useOsReadBuffering;
            _store = new FasterKV<Key, Value>(1L << 20, logSettings);
            _sessionPool = new AsyncPool<ClientSession<Key, Value, Value, Value, Empty, SimpleFunctions<Key, Value, Empty>>>(
                    logSettings.LogDevice.ThrottleLimit,
                    () => _store.For(new SimpleFunctions<Key, Value, Empty>()).NewSession<SimpleFunctions<Key, Value, Empty>>());
        }

        public async ValueTask UpsertAsync(Key key, Value value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync().ConfigureAwait(false);
            var r = await session.UpsertAsync(key, value).ConfigureAwait(false);
            while (r.Status == Status.PENDING)
            {
                Interlocked.Increment(ref upsertPendingCount);
                r = await r.CompleteAsync().ConfigureAwait(false);
            }
            _sessionPool.Return(session);
        }

        public void Upsert(Key key, Value value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();
            var status = session.Upsert(key, value);
            Assert.True(status != Status.PENDING);
            _sessionPool.Return(session);
        }

        public async ValueTask UpsertChunkAsync((Key, Value)[] chunk, int offset, int count)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync().ConfigureAwait(false);

            for (var i = 0; i < count; ++i)
            {
                var r = await session.UpsertAsync(chunk[offset + i].Item1, chunk[offset + i].Item2);
                while (r.Status == Status.PENDING)
                {
                    Interlocked.Increment(ref upsertPendingCount);
                    r = await r.CompleteAsync().ConfigureAwait(false);
                }
            }
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
            if (result.status == Status.PENDING)
            {
                session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                int count = 0;
                for (; completedOutputs.Next(); ++count)
                {
                    Assert.Equal(key, completedOutputs.Current.Key);
                    result = (Status.OK, completedOutputs.Current.Output);
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
