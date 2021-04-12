using FASTER.core;
using Xunit;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace AsyncStress
{
    public class FasterWrapper
    {
        readonly FasterKV<int, int> _store;
        readonly AsyncPool<ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>>> _sessionPool;
        
        // OS Buffering is safe to use in this app because Reads are done after all updates
        internal static bool useOsReadBuffering = false;

        public FasterWrapper()
        {
            var logDirectory ="d:/FasterLogs";
            var logFileName = Guid.NewGuid().ToString();
            var logSettings = new LogSettings
            {
                LogDevice = new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true, osReadBuffering: useOsReadBuffering),
                PageSizeBits = 12,
                MemorySizeBits = 13
            };

            Console.WriteLine($"    Using {logSettings.LogDevice.GetType()}");

            _store = new FasterKV<int, int>(1L << 20, logSettings);
            _sessionPool = new AsyncPool<ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>>>(
                    logSettings.LogDevice.ThrottleLimit, 
                    () => _store.For(new SimpleFunctions<int, int, Empty>()).NewSession(new SimpleFunctions<int, int, Empty>())
            );
        }

        // This can be used to verify the same amount data is loaded.
        public long TailAddress => _store.Log.TailAddress;

        // Indicates how many operations went pending
        public int UpsertPendingCount = 0;
        public int ReadPendingCount = 0;

        public async ValueTask UpsertAsync(int key, int value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync();
            var r = await session.UpsertAsync(key, value);
            while (r.Status == Status.PENDING)
            {
                Interlocked.Increment(ref UpsertPendingCount);
                r = await r.CompleteAsync();
            }
            _sessionPool.Return(session);
        }

        public void Upsert(int key, int value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();
            var status = session.Upsert(key, value);
            if (status == Status.PENDING)
            {
                // This should not happen for sync Upsert().
                Interlocked.Increment(ref UpsertPendingCount);
                session.CompletePending();
            }
            _sessionPool.Return(session);
        }

        public async ValueTask UpsertChunkAsync(int start, int count)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();

            for (var ii = 0; ii < count; ++ii)
            {
                var key = start + ii;
                var r = await session.UpsertAsync(key, key);
                while (r.Status == Status.PENDING)
                {
                    Interlocked.Increment(ref UpsertPendingCount);
                    r = await r.CompleteAsync();
                }
            }
            _sessionPool.Return(session);
        }

        public async ValueTask<(Status, int)> ReadAsync(int key)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync();
            var result = (await session.ReadAsync(key).ConfigureAwait(false)).Complete();
            _sessionPool.Return(session);
            return result;
        }

        public ValueTask<(Status, int)> Read(int key)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();
            var result = session.Read(key);
            if (result.status == Status.PENDING)
            {
                Interlocked.Increment(ref ReadPendingCount);
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
            return new ValueTask<(Status, int)>(result);
        }

        public async ValueTask ReadChunkAsync(int start, int count, ValueTask<(Status, int)>[] readTasks)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();

            for (var ii = 0; ii < count; ++ii)
            {
                var key = start + ii;
                readTasks[key] = new ValueTask<(Status, int)>((await session.ReadAsync(key).ConfigureAwait(false)).Complete());
            }
            _sessionPool.Return(session);
        }

        public void Dispose()
        {
            _sessionPool.Dispose();
            _store.Dispose();
        }
    }
}
