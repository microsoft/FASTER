using FASTER.core;
using Xunit;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncStress
{
    public class FasterWrapper<Key, Value> : IFasterWrapper<Key, Value>
    {
        readonly FasterKV<Key, Value> _store;
        readonly AsyncPool<ClientSession<Key, Value, Value, Value, Empty, SimpleFunctions<Key, Value, Empty>>> _sessionPool;
        readonly bool useOsReadBuffering;
        int upsertPendingCount = 0;
        int readPendingCount = 0;

        // This can be used to verify the same amount data is loaded.
        public long TailAddress => _store.Log.TailAddress;

        // Indicates how many operations went pending
        public int UpsertPendingCount { get => upsertPendingCount; set => upsertPendingCount = value; }
        public int ReadPendingCount { get => readPendingCount; set => readPendingCount = value; }
        // Whether OS Read buffering is enabled
        public bool UseOsReadBuffering => useOsReadBuffering;

        public FasterWrapper(bool useOsReadBuffering = false)
        {
            var logDirectory = "d:/FasterLogs";
            var logFileName = Guid.NewGuid().ToString();
            var logSettings = new LogSettings
            {
                LogDevice = new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true, osReadBuffering: useOsReadBuffering),
                ObjectLogDevice = new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true, osReadBuffering: useOsReadBuffering),
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
                session = await _sessionPool.GetAsync();
            var r = await session.UpsertAsync(key, value);
            while (r.Status == Status.PENDING)
            {
                Interlocked.Increment(ref upsertPendingCount);
                r = await r.CompleteAsync();
            }
            _sessionPool.Return(session);
        }

        public void Upsert(Key key, Value value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();
            var status = session.Upsert(key, value);
            if (status == Status.PENDING)
            {
                // This should not happen for sync Upsert().
                Interlocked.Increment(ref upsertPendingCount);
                session.CompletePending();
            }
            _sessionPool.Return(session);
        }

        public async ValueTask UpsertChunkAsync((Key, Value)[] chunk)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();

            for (var ii = 0; ii < chunk.Length; ++ii)
            {
                var r = await session.UpsertAsync(chunk[ii].Item1, chunk[ii].Item2);
                while (r.Status == Status.PENDING)
                {
                    Interlocked.Increment(ref upsertPendingCount);
                    r = await r.CompleteAsync();
                }
            }
            _sessionPool.Return(session);
        }

        public async ValueTask<(Status, Value)> ReadAsync(Key key)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync();
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
                Interlocked.Increment(ref readPendingCount);
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

        public async ValueTask ReadChunkAsync(Key[] chunk, ValueTask<(Status, Value)>[] results, int offset)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();

            // Reads in chunk are performed serially
            for (var ii = 0; ii < chunk.Length; ++ii)
                results[offset + ii] = new ValueTask<(Status, Value)>((await session.ReadAsync(chunk[ii])).Complete());
            _sessionPool.Return(session);
        }

        public async ValueTask<(Status, Value)[]> ReadChunkAsync(Key[] chunk)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();

            // Reads in chunk are performed serially
            (Status, Value)[] result = new (Status, Value)[chunk.Length];
            for (var ii = 0; ii < chunk.Length; ++ii)
                result[ii] = (await session.ReadAsync(chunk[ii]).ConfigureAwait(false)).Complete();
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
