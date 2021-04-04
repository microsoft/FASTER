using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace AsyncStress
{
    public class FasterWrapper
    {
        private readonly FasterKV<int, int> store;
        private readonly SimpleFunctions<int, int, Empty> _simpleFunctions;
        private readonly ConcurrentQueue<ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>>> _sessionPool;
        private readonly FasterKV<int, int>.ClientSessionBuilder<int, int, Empty> _clientSessionBuilder;

        public int NumSessions => _sessionPool.Count;

        public FasterWrapper()
        {
            _simpleFunctions = new SimpleFunctions<int, int, Empty>();
            _sessionPool = new ConcurrentQueue<ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>>>();

            string logDirectory ="d:/FasterLogs";
            string logFileName = Guid.NewGuid().ToString();
            var logSettings = new LogSettings
            {
                LogDevice = new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true),
                PageSizeBits = 12,
                MemorySizeBits = 13
            };

            store = new FasterKV<int, int>(1L << 20, logSettings);
            _clientSessionBuilder = store.For(_simpleFunctions);
        }

        public async Task UpsertAsync(int key, int value)
        {
            var session = GetPooledSession();
            var r = await session.UpsertAsync(key, value);
            while (r.Status == Status.PENDING)
                r = await r.CompleteAsync();
            _sessionPool.Enqueue(session);
        }

        public async Task<(Status, int?)> ReadAsync(int key)
        {
            var session = GetPooledSession();
            (Status, int) result = (await session.ReadAsync(key).ConfigureAwait(false)).Complete();
            _sessionPool.Enqueue(session);
            return result;
        }

        private ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>> GetPooledSession()
        {
            if (_sessionPool.TryDequeue(out var result))
                return result;
            return _clientSessionBuilder.NewSession<SimpleFunctions<int, int, Empty>>();
        }

        public void Dispose()
        {
            foreach (var session in _sessionPool)
                session.Dispose();
            store.Dispose();
        }
    }
}
