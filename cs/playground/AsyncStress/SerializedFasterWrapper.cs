using Xunit;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Buffers;

using FASTER.core;
using MessagePack;

namespace AsyncStress
{
    public class SerializedFasterWrapper<Key, Value> : IFasterWrapper<Key, Value>
    {
        readonly FasterKV<SpanByte, SpanByte> _store;
        readonly AsyncPool<ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions>> _sessionPool;
        readonly bool useOsReadBuffering;
        int upsertPendingCount = 0;

        // This can be used to verify the same amount data is loaded.
        public long TailAddress => _store.Log.TailAddress;

        // Indicates how many operations went pending
        public int UpsertPendingCount { get => upsertPendingCount; set => upsertPendingCount = value; }
        // Whether OS Read buffering is enabled
        public bool UseOsReadBuffering => useOsReadBuffering;

        public SerializedFasterWrapper(bool useOsReadBuffering = false)
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
            _store = new FasterKV<SpanByte, SpanByte>(1L << 20, logSettings);
            _sessionPool = new AsyncPool<ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions>>(
                    logSettings.LogDevice.ThrottleLimit,
                    () => _store.For(new SpanByteFunctions()).NewSession<SpanByteFunctions>());
        }

        public async ValueTask UpsertAsync(Key key, Value value)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync();

            byte[] keyBytes = MessagePackSerializer.Serialize(key);
            byte[] valueBytes = MessagePackSerializer.Serialize(value);
            ValueTask<FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<SpanByte, SpanByteAndMemory, Empty>> task;

            unsafe
            {
                fixed (byte* kb = keyBytes)
                {
                    fixed (byte* vb = valueBytes)
                    {
                        var keySpanByte = SpanByte.FromPointer(kb, keyBytes.Length);
                        var valueSpanByte = SpanByte.FromPointer(vb, valueBytes.Length);
                        task = session.UpsertAsync(ref keySpanByte, ref valueSpanByte);
                    }
                }
            }
            var r = await task;
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

            byte[] keyBytes = MessagePackSerializer.Serialize(key);
            byte[] valueBytes = MessagePackSerializer.Serialize(value);
            Status status;

            unsafe
            {
                fixed (byte* kb = keyBytes)
                {
                    fixed (byte* vb = valueBytes)
                    {
                        var keySpanByte = SpanByte.FromPointer(kb, keyBytes.Length);
                        var valueSpanByte = SpanByte.FromPointer(vb, valueBytes.Length);
                        status = session.Upsert(ref keySpanByte, ref valueSpanByte);
                    }
                }
            }

            Assert.True(status != Status.PENDING);
            _sessionPool.Return(session);
        }

        public async ValueTask UpsertChunkAsync((Key, Value)[] chunk, int offset, int count)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();

            for (var i = 0; i < count; ++i)
            {
                byte[] keyBytes = MessagePackSerializer.Serialize(chunk[offset + i].Item1);
                byte[] valueBytes = MessagePackSerializer.Serialize(chunk[offset + i].Item2);
                ValueTask<FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<SpanByte, SpanByteAndMemory, Empty>> task;

                unsafe
                {
                    fixed (byte* kb = keyBytes)
                    {
                        fixed (byte* vb = valueBytes)
                        {
                            var keySpanByte = SpanByte.FromPointer(kb, keyBytes.Length);
                            var valueSpanByte = SpanByte.FromPointer(vb, valueBytes.Length);
                            task = session.UpsertAsync(ref keySpanByte, ref valueSpanByte);
                        }
                    }
                }

                var r = await task;
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

            byte[] keyBytes = MessagePackSerializer.Serialize(key);
            ValueTask<FasterKV<SpanByte, SpanByte>.ReadAsyncResult<SpanByte, SpanByteAndMemory, Empty>> task;

            unsafe
            {
                fixed (byte* kb = keyBytes)
                {
                    var keySpanByte = SpanByte.FromPointer(kb, keyBytes.Length);
                    task = session.ReadAsync(ref keySpanByte);
                }
            }

            var (status, output) = (await task.ConfigureAwait(false)).Complete();
            _sessionPool.Return(session);

            using IMemoryOwner<byte> memoryOwner = output.Memory;

            return (status, status != Status.OK ? default : MessagePackSerializer.Deserialize<Value>(memoryOwner.Memory));
        }

        public ValueTask<(Status, Value)> Read(Key key)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();

            byte[] keyBytes = MessagePackSerializer.Serialize(key);
            (Status, SpanByteAndMemory) result;
            (Status, Value) userResult = default;

            unsafe
            {
                fixed (byte* kb = keyBytes)
                {
                    var keySpanByte = SpanByte.FromPointer(kb, keyBytes.Length);
                    result = session.Read(keySpanByte);
                }
            }

            if (result.Item1 == Status.PENDING)
            {
                session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                int count = 0;
                for (; completedOutputs.Next(); ++count)
                {
                    using IMemoryOwner<byte> memoryOwner = completedOutputs.Current.Output.Memory;
                    userResult = (completedOutputs.Current.Status, completedOutputs.Current.Status != Status.OK ? default : MessagePackSerializer.Deserialize<Value>(memoryOwner.Memory));
                }
                completedOutputs.Dispose();
                Assert.Equal(1, count);
            }
            else
            {
                using IMemoryOwner<byte> memoryOwner = result.Item2.Memory;
                userResult = (result.Item1, result.Item1 != Status.OK ? default : MessagePackSerializer.Deserialize<Value>(memoryOwner.Memory));
            }
            _sessionPool.Return(session);
            return new ValueTask<(Status, Value)>(userResult);
        }

        public async ValueTask<(Status, Value)[]> ReadChunkAsync(Key[] chunk, int offset, int count)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().GetAwaiter().GetResult();

            // Reads in chunk are performed serially
            (Status, Value)[] result = new (Status, Value)[count];
            for (var i = 0; i < count; ++i)
                result[i] = await ReadAsync(chunk[offset + i]).ConfigureAwait(false);

            _sessionPool.Return(session);
            return result;
        }


        public void Dispose()
        {
            _sessionPool.Dispose();
            _store.Dispose();
        }
    }

    public class SpanByteFunctions : SpanByteFunctions<SpanByte, SpanByteAndMemory, Empty>
    {
        public unsafe override void SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
        {
            value.CopyTo(ref dst, MemoryPool<byte>.Shared);
        }

        public unsafe override void ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
        {
            value.CopyTo(ref dst, MemoryPool<byte>.Shared);
        }
    }
}
