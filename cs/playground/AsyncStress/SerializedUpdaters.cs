using System.Threading.Tasks;

using FASTER.core;

namespace AsyncStress
{
    public partial class SerializedFasterWrapper<Key, Value> : IFasterWrapper<Key, Value>
    {
        public interface IUpdater<TAsyncResult>
        {
            Status Update(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions, DefaultStoreFunctions<SpanByte, SpanByte>> session, ref SpanByte key, ref SpanByte value);

            ValueTask<TAsyncResult> UpdateAsync(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions, DefaultStoreFunctions<SpanByte, SpanByte>> session, ref SpanByte key, ref SpanByte value);

            ValueTask<int> CompleteAsync(TAsyncResult result);
        }

        internal struct UpsertUpdater : IUpdater<FasterKV<SpanByte, SpanByte, DefaultStoreFunctions<SpanByte, SpanByte>>.UpsertAsyncResult<SpanByte, SpanByteAndMemory, Empty>>
        {
            public Status Update(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions, DefaultStoreFunctions<SpanByte, SpanByte>> session, ref SpanByte key, ref SpanByte value)
                => session.Upsert(ref key, ref value);

            public ValueTask<FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<SpanByte, SpanByteAndMemory, Empty>> UpdateAsync(
                    ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions, DefaultStoreFunctions<SpanByte, SpanByte>> session, ref SpanByte key, ref SpanByte value)
                => session.UpsertAsync(ref key, ref value);

            public async ValueTask<int> CompleteAsync(FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<SpanByte, SpanByteAndMemory, Empty> result)
            {
                var numPending = 0;
                for (; result.Status.IsPending; ++numPending)
                    result = await result.CompleteAsync().ConfigureAwait(false);
                return numPending;
            }
        }

        internal struct RmwUpdater : IUpdater<FasterKV<SpanByte, SpanByte, DefaultStoreFunctions<SpanByte, SpanByte>>.RmwAsyncResult<SpanByte, SpanByteAndMemory, Empty>>
        {
            public Status Update(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions, DefaultStoreFunctions<SpanByte, SpanByte>> session, ref SpanByte key, ref SpanByte value)
                => session.RMW(ref key, ref value);

            public ValueTask<FasterKV<SpanByte, SpanByte>.RmwAsyncResult<SpanByte, SpanByteAndMemory, Empty>> UpdateAsync(
                    ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions, DefaultStoreFunctions<SpanByte, SpanByte>> session, ref SpanByte key, ref SpanByte value)
                => session.RMWAsync(ref key, ref value);

            public async ValueTask<int> CompleteAsync(FasterKV<SpanByte, SpanByte>.RmwAsyncResult<SpanByte, SpanByteAndMemory, Empty> result)
            {
                var numPending = 0;
                for (; result.Status.IsPending; ++numPending)
                    result = await result.CompleteAsync().ConfigureAwait(false);
                return numPending;
            }
        }
    }
}