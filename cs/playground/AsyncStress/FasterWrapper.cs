using FASTER.core;
using System;
using System.IO;
using System.Threading.Tasks;

namespace AsyncStress
{
    public class FasterWrapper
    {
        readonly FasterKV<int, int> _store;
        readonly AsyncPool<ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>>> _sessionPool;

        public FasterWrapper()
        {
            var logDirectory ="d:/FasterLogs";
            var logFileName = Guid.NewGuid().ToString();
            var logSettings = new LogSettings
            {
                LogDevice = new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true),
                PageSizeBits = 12,
                MemorySizeBits = 13
            };

            Console.WriteLine($"    Using {logSettings.LogDevice.GetType()}");

            _store = new FasterKV<int, int>(1L << 20, logSettings);
            _sessionPool = new AsyncPool<ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>>>(logSettings.LogDevice.ThrottleLimit, () => _store.For(new SimpleFunctions<int, int, Empty>()).NewSession(new SimpleFunctions<int, int, Empty>()));
        }

        public async Task UpsertAsync(int key, int value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync();
            var r = await session.UpsertAsync(key, value);
            while (r.Status == Status.PENDING)
                r = await r.CompleteAsync();
            _sessionPool.Return(session);
        }

        public async Task<(Status, int)> ReadAsync(int key)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync();
            var result = (await session.ReadAsync(key).ConfigureAwait(false)).Complete();
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
