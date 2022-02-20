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
    public partial class SerializedFasterWrapper<Key, Value> : IFasterWrapper<Key, Value>
    {
        readonly FasterKV<SpanByte, SpanByte> _store;
        readonly AsyncPool<ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions>> _sessionPool;
        readonly UpsertUpdater upsertUpdater = new();
        readonly RmwUpdater rmwUpdater = new();
        readonly bool useOsReadBuffering;
        int pendingCount = 0;

        // This can be used to verify the same amount data is loaded.
        public long TailAddress => _store.Log.TailAddress;

        // Indicates how many operations went pending
        public int PendingCount { get => pendingCount; set => pendingCount = value; }

        public void ClearPendingCount() => pendingCount = 0;

        // Whether OS Read buffering is enabled
        public bool UseOsReadBuffering => useOsReadBuffering;

        public SerializedFasterWrapper(bool useLargeLog, bool useOsReadBuffering = false)
        {
            var logDirectory = "d:/FasterLogs";
            var logFileName = Guid.NewGuid().ToString();
            var logSettings = new LogSettings
            {
                LogDevice = new ManagedLocalStorageDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true, osReadBuffering: useOsReadBuffering)
            };

            if (!useLargeLog)
            {
                logSettings.PageSizeBits = 12;
                logSettings.MemorySizeBits = 13;
            }

            Console.WriteLine($"    SerializedFasterWrapper using {logSettings.LogDevice.GetType()} and {(useLargeLog ? "large" : "small")} memory log");

            this.useOsReadBuffering = useOsReadBuffering;
            _store = new FasterKV<SpanByte, SpanByte>(1L << 20, logSettings);
            _sessionPool = new AsyncPool<ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions>>(
                    logSettings.LogDevice.ThrottleLimit,
                    () => _store.For(new SpanByteFunctions()).NewSession<SpanByteFunctions>());
        }

        public ValueTask UpsertAsync(Key key, Value value) 
            => this.UpdateAsync<UpsertUpdater, FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<SpanByte, SpanByteAndMemory, Empty>>(this.upsertUpdater, key, value);

        public void Upsert(Key key, Value value)
            => this.Update<UpsertUpdater, FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<SpanByte, SpanByteAndMemory, Empty>>(this.upsertUpdater, key, value);

        public ValueTask UpsertChunkAsync((Key, Value)[] chunk, int offset, int count)
            => this.UpdateChunkAsync<UpsertUpdater, FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<SpanByte, SpanByteAndMemory, Empty>>(this.upsertUpdater, chunk, offset, count);

        public ValueTask RMWAsync(Key key, Value value)
            => this.UpdateAsync<RmwUpdater, FasterKV<SpanByte, SpanByte>.RmwAsyncResult<SpanByte, SpanByteAndMemory, Empty>>(this.rmwUpdater, key, value);

        public void RMW(Key key, Value value)
            => this.Update<RmwUpdater, FasterKV<SpanByte, SpanByte>.RmwAsyncResult<SpanByte, SpanByteAndMemory, Empty>>(this.rmwUpdater, key, value);

        public ValueTask RMWChunkAsync((Key, Value)[] chunk, int offset, int count)
            => this.UpdateChunkAsync<RmwUpdater, FasterKV<SpanByte, SpanByte>.RmwAsyncResult<SpanByte, SpanByteAndMemory, Empty>>(this.rmwUpdater, chunk, offset, count);

        internal async ValueTask UpdateAsync<TUpdater, TAsyncResult>(TUpdater updater, Key key, Value value)
            where TUpdater : IUpdater<TAsyncResult>
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync().ConfigureAwait(false);

            byte[] keyBytes = MessagePackSerializer.Serialize(key);
            byte[] valueBytes = MessagePackSerializer.Serialize(value);
            ValueTask<TAsyncResult> task;

            unsafe
            {
                fixed (byte* kb = keyBytes)
                {
                    fixed (byte* vb = valueBytes)
                    {
                        var keySpanByte = SpanByte.FromPointer(kb, keyBytes.Length);
                        var valueSpanByte = SpanByte.FromPointer(vb, valueBytes.Length);
                        task = updater.UpdateAsync(session, ref keySpanByte, ref valueSpanByte);
                    }
                }
            }
            Interlocked.Add(ref pendingCount, await updater.CompleteAsync(await task.ConfigureAwait(false)));
            _sessionPool.Return(session);
        }

        public void Update<TUpdater, TAsyncResult>(TUpdater updater, Key key, Value value)
            where TUpdater : IUpdater<TAsyncResult>
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().AsTask().GetAwaiter().GetResult();

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
                        status = updater.Update(session, ref keySpanByte, ref valueSpanByte);
                    }
                }
            }

            Assert.False(status.Pending);
            _sessionPool.Return(session);
        }

        public async ValueTask UpdateChunkAsync<TUpdater, TAsyncResult>(TUpdater updater, (Key, Value)[] chunk, int offset, int count)
            where TUpdater : IUpdater<TAsyncResult>
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().AsTask().GetAwaiter().GetResult();

            for (var i = 0; i < count; ++i)
            {
                byte[] keyBytes = MessagePackSerializer.Serialize(chunk[offset + i].Item1);
                byte[] valueBytes = MessagePackSerializer.Serialize(chunk[offset + i].Item2);
                ValueTask<TAsyncResult> task;

                unsafe
                {
                    fixed (byte* kb = keyBytes)
                    {
                        fixed (byte* vb = valueBytes)
                        {
                            var keySpanByte = SpanByte.FromPointer(kb, keyBytes.Length);
                            var valueSpanByte = SpanByte.FromPointer(vb, valueBytes.Length);
                            task = updater.UpdateAsync(session, ref keySpanByte, ref valueSpanByte);
                        }
                    }
                }
                Interlocked.Add(ref pendingCount, await updater.CompleteAsync(await task.ConfigureAwait(false)));
            }
            _sessionPool.Return(session);
        }

        public async ValueTask<(Status, Value)> ReadAsync(Key key)
        {
            if (!_sessionPool.TryGet(out var session))
                session = await _sessionPool.GetAsync().AsTask().ConfigureAwait(false);

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
            Assert.True(status.CompletedSuccessfully);

            using IMemoryOwner<byte> memoryOwner = output.Memory;

            return (status, status.Found ? MessagePackSerializer.Deserialize<Value>(memoryOwner.Memory) : default);
        }

        public ValueTask<(Status, Value)> Read(Key key)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().AsTask().GetAwaiter().GetResult();

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

            if (result.Item1.Pending)
            {
                session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                int count = 0;
                for (; completedOutputs.Next(); ++count)
                {
                    using IMemoryOwner<byte> memoryOwner = completedOutputs.Current.Output.Memory;
                    Assert.True(completedOutputs.Current.Status.CompletedSuccessfully);
                    userResult = (completedOutputs.Current.Status, completedOutputs.Current.Status.Found ? MessagePackSerializer.Deserialize<Value>(memoryOwner.Memory) : default);
                }
                completedOutputs.Dispose();
                Assert.Equal(1, count);
            }
            else
            {
                using IMemoryOwner<byte> memoryOwner = result.Item2.Memory;
                Assert.True(result.Item1.CompletedSuccessfully);
                userResult = (result.Item1, result.Item1.Found ? MessagePackSerializer.Deserialize<Value>(memoryOwner.Memory) : default);
            }
            _sessionPool.Return(session);
            return new ValueTask<(Status, Value)>(userResult);
        }

        public async ValueTask<(Status, Value)[]> ReadChunkAsync(Key[] chunk, int offset, int count)
        {
            if (!_sessionPool.TryGet(out var session))
                session = _sessionPool.GetAsync().AsTask().GetAwaiter().GetResult();

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
        public unsafe override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            value.CopyTo(ref dst, MemoryPool<byte>.Shared);
            return true;
        }

        public unsafe override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            value.CopyTo(ref dst, MemoryPool<byte>.Shared);
            return true;
        }
    }
}
